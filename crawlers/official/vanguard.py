"""Cardfight!! Vanguard official card list crawler.

Crawl procedure
---------------
1. GET https://cf-vanguard.com/cardlist/ → parse all product-item divs to get
   sets: expansion_id (numeric), set_code (e.g. "DZ-BT13"), set_name, category,
   release_date.
2. For each expansion, paginate the AJAX endpoint:
     /cardlist/cardsearch_ex/?expansion=NNN&view=text&page=P&sort=no
   to discover all card numbers (24 per page; stop when a page returns nothing).
3. For each card number, fetch the detail page:
     /cardlist/?cardno=DZ-BT13/001&expansion=NNN
   to get: rarity_code, card_name, card_type, nation, race, grade, power,
   shield, critical, skill, illustrator.
4. Post-crawl: group cards sharing the same name within a set (different card
   numbers correspond to different rarity printings of the same logical card)
   and update card_base_id to the canonical (lexicographically first) number.

HTML structure (confirmed from live site)
-----------------------------------------
Set list (/cardlist/):
  <div class="product-item ... product-id-290 product-type-booster">
    <a href="/cardlist/cardsearch/?expansion=290">
      <div class="title">【DZ-BT13】「幻真星戦」</div>
      <div class="category booster">ブースターパック</div>
      <div class="release">2026/02/13(金) 発売</div>
    </a>
  </div>

Card list AJAX (/cardlist/cardsearch_ex/?expansion=290&view=text&page=1&sort=no):
  <li class="ex-item">
    <a href="/cardlist/?cardno=DZ-BT13/001&expansion=290&view=text">
      <div class="number">DZ-BT13/001</div>
      <h5>穿雷竜 アルファルド "幻影"<span>（ルビ）</span></h5>
      <div class="status">ノーマルユニット<span>｜</span>ドラゴンエンパイア...</div>
    </a>
  </li>

Card detail (/cardlist/?cardno=DZ-BT13/001&expansion=290):
  <div class="cardlist_detail">
    <div class="name"><span class="face">穿雷竜 アルファルド "幻影"</span></div>
    <div class="text-list">
      <div class="type">ノーマルユニット</div>
      <div class="nation">ドラゴンエンパイア</div>
      <div class="race">サンダードラゴン/幻影</div>
      <div class="grade">グレード 3</div>
      <div class="power">パワー 13000</div>
      <div class="critical">クリティカル 1</div>
      <div class="shield">シールド -</div>
      <div class="skill">ツインドライブ、ペルソナライド</div>
    </div>
    <div class="text-list">
      <div class="number">DZ-BT13/001</div>
      <div class="rarity">RRR</div>
      <div class="illstrator">なかざき冬</div>
    </div>
  </div>

Numbering scheme: unique_per_rarity
  - DZ-BT13/001  → rarity RRR  (穿雷竜 アルファルド "幻影")
  - DZ-BT13/SR01 → rarity SR   (same logical card, different printing)
  - DZ-BT13/T01  → trigger cards have their own numbering range
  Card numbers uniquely identify a rarity variant; same logical card is
  grouped by card_name via post-crawl update of card_base_id.

Performance note
----------------
Each card requires one HTTP request for its detail page.
A large booster (288 cards) takes ~7 minutes at 1.5 s/request.
Full database (~250+ expansions) takes many hours — use --set to test.
"""

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Iterator

import duckdb
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from crawlers.official.base import OfficialCard, OfficialCrawler
from crawlers.storage import (
    DB_PATH,
    get_connection,
    init_schema,
    insert_official_cards,
)

logger = logging.getLogger(__name__)

VG_BASE = "https://cf-vanguard.com"
CARD_LIST_URL = f"{VG_BASE}/cardlist/"
CARD_SEARCH_EX_URL = f"{VG_BASE}/cardlist/cardsearch_ex/"
CARD_DETAIL_URL = f"{VG_BASE}/cardlist/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
}

# Matches the set code inside 【...】, e.g. "DZ-BT13" from 【DZ-BT13】「幻真星戦」
_SET_CODE_RE = re.compile(r"【([^】]+)】")
# Matches YYYY/MM/DD in release date text
_RELEASE_DATE_RE = re.compile(r"(\d{4}/\d{2}/\d{2})")

PAGE_SIZE = 24  # cards returned per cardsearch_ex page


@dataclass
class VanguardSet:
    expansion_id: int
    set_code: str
    set_name: str
    set_title: str      # full title as shown on site, e.g. 【DZ-BT13】「幻真星戦」
    category: str       # booster | trial | pr | ...
    release_date: str | None


class VanguardOfficialCrawler(OfficialCrawler):
    """Crawler for the Cardfight!! Vanguard official card database at cf-vanguard.com."""

    tcg = "vanguard"

    def __init__(self, delay: float = 1.5):
        """
        Args:
            delay: Seconds to sleep between HTTP requests (be polite).
        """
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._sets: list[VanguardSet] = []

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> BeautifulSoup:
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        time.sleep(self.delay)
        return BeautifulSoup(resp.text, "lxml")

    # ------------------------------------------------------------------
    # Metadata: sets
    # ------------------------------------------------------------------

    def _fetch_sets(self) -> list[VanguardSet]:
        """Parse all expansion products from the main card list page."""
        logger.info("Fetching set list from %s", CARD_LIST_URL)
        soup = self._get(CARD_LIST_URL)
        sets: list[VanguardSet] = []

        for item in soup.find_all("div", class_=lambda c: c and "product-item" in c):
            link = item.find("a", href=True)
            if not link:
                continue
            m = re.search(r"expansion=(\d+)", link["href"])
            if not m:
                continue
            expansion_id = int(m.group(1))

            title_div = item.find("div", class_="title")
            if not title_div:
                continue
            set_title = title_div.get_text(strip=True)

            # Extract set_code from 【DZ-BT13】 pattern
            cm = _SET_CODE_RE.search(set_title)
            if cm:
                set_code = cm.group(1)
                # set_name is everything after the last 】, stripped of 「」
                set_name = set_title[cm.end():].strip().strip("「」").strip()
            else:
                # PR cards, etc. — no bracketed code; use expansion ID as key
                set_code = f"VG-EX{expansion_id}"
                set_name = set_title

            cat_div = item.find("div", class_="category")
            category = ""
            if cat_div:
                for cls in cat_div.get("class", []):
                    if cls != "category":
                        category = cls
                        break

            release_date = None
            rel_div = item.find("div", class_="release")
            if rel_div:
                dm = _RELEASE_DATE_RE.search(rel_div.get_text())
                if dm:
                    release_date = dm.group(1).replace("/", "-")

            sets.append(VanguardSet(
                expansion_id=expansion_id,
                set_code=set_code,
                set_name=set_name,
                set_title=set_title,
                category=category,
                release_date=release_date,
            ))

        self._sets = sets
        logger.info("Found %d expansions", len(sets))
        return sets

    def crawl_sets(self) -> Iterator[dict]:
        if not self._sets:
            self._fetch_sets()
        for s in self._sets:
            yield {
                "set_code": s.set_code,
                "set_name": s.set_name,
                "set_title": s.set_title,
                "expansion_id": s.expansion_id,
                "category": s.category,
                "release_date": s.release_date,
            }

    # ------------------------------------------------------------------
    # Card number discovery via cardsearch_ex
    # ------------------------------------------------------------------

    def _list_card_numbers(self, expansion_id: int) -> list[str]:
        """Paginate cardsearch_ex to collect all card numbers for an expansion."""
        card_numbers: list[str] = []
        page = 1
        while True:
            soup = self._get(
                CARD_SEARCH_EX_URL,
                params={"expansion": expansion_id, "view": "text", "page": page, "sort": "no"},
            )
            nums = [
                div.get_text(strip=True)
                for div in soup.find_all("div", class_="number")
                if div.get_text(strip=True)
            ]
            if not nums:
                break
            card_numbers.extend(nums)
            logger.debug("    page %d: %d cards", page, len(nums))
            if len(nums) < PAGE_SIZE:
                break  # last page
            page += 1
        return card_numbers

    # ------------------------------------------------------------------
    # Card detail parsing
    # ------------------------------------------------------------------

    def _fetch_card_detail(self, card_number: str, expansion_id: int) -> dict | None:
        """Fetch the card detail page and return all parsed fields."""
        soup = self._get(
            CARD_DETAIL_URL,
            params={"cardno": card_number, "expansion": expansion_id},
        )
        detail = soup.find("div", class_="cardlist_detail")
        if not detail:
            logger.warning("No cardlist_detail element for %s (expansion %d)", card_number, expansion_id)
            return None

        def _text(cls: str) -> str:
            el = detail.find("div", class_=cls)
            return el.get_text(strip=True) if el else ""

        name_el = detail.find("span", class_="face")
        card_name = name_el.get_text(strip=True) if name_el else ""

        # Card image: div.image > div.main > img
        image_url = ""
        image_div = detail.find("div", class_="image")
        if image_div:
            img = image_div.find("img")
            if img:
                src = img.get("src", "")
                image_url = f"{VG_BASE}{src}" if src.startswith("/") else src

        # Grade/power/shield values include a Japanese prefix — strip it
        grade_raw = _text("grade").removeprefix("グレード").strip()
        power_raw = _text("power").removeprefix("パワー").strip()
        shield_raw = _text("shield").removeprefix("シールド").strip()
        critical_raw = _text("critical").removeprefix("クリティカル").strip()

        effect_div = detail.find("div", class_="effect")
        effect_text = effect_div.get_text(strip=True) if effect_div else ""

        flavor_div = detail.find("div", class_="flavor")
        flavor_text = flavor_div.get_text(strip=True) if flavor_div else ""

        return {
            "card_name": card_name,
            "rarity_code": _text("rarity"),
            "card_type": _text("type"),
            "nation": _text("nation"),
            "race": _text("race"),
            "grade": grade_raw,
            "power": power_raw,
            "shield": shield_raw,
            "critical": critical_raw,
            "skill": _text("skill"),
            "illustrator": _text("illstrator"),  # note: official site has this typo
            "effect": effect_text,
            "flavor": flavor_text,
            "image_url": image_url,
        }

    # ------------------------------------------------------------------
    # crawl_cards
    # ------------------------------------------------------------------

    def crawl_cards(self, set_code: str) -> Iterator[OfficialCard]:
        """Yield all OfficialCard records for a set.

        Phase 1: discover card numbers via cardsearch_ex.
        Phase 2: fetch each card's detail page for rarity and full data.
        """
        if not self._sets:
            self._fetch_sets()

        vg_set = next((s for s in self._sets if s.set_code == set_code), None)
        if not vg_set:
            raise ValueError(f"Unknown set code {set_code!r}. Call crawl_sets() first.")

        logger.info(
            "Discovering card numbers for %s (expansion_id=%d)",
            set_code, vg_set.expansion_id,
        )
        card_numbers = self._list_card_numbers(vg_set.expansion_id)
        logger.info("  found %d card numbers, fetching details...", len(card_numbers))

        for card_number in tqdm(card_numbers, desc=vg_set.set_code, unit="card", leave=False):
            detail = self._fetch_card_detail(card_number, vg_set.expansion_id)
            if detail is None:
                continue

            rarity_code = detail["rarity_code"] or ""
            card_name = detail["card_name"] or card_number

            extra = {
                "expansion_id": vg_set.expansion_id,
                "card_type": detail["card_type"],
                "nation": detail["nation"],
                "race": detail["race"],
                "grade": detail["grade"],
                "power": detail["power"],
                "shield": detail["shield"],
                "critical": detail["critical"],
                "skill": detail["skill"],
                "illustrator": detail["illustrator"],
                "effect": detail["effect"],
                "flavor": detail["flavor"],
                "image_url": detail["image_url"],
                "category": vg_set.category,
                "release_date": vg_set.release_date,
            }

            yield OfficialCard(
                tcg="vanguard",
                set_code=vg_set.set_code,
                set_name=vg_set.set_name,
                card_number=card_number,
                card_name=card_name,
                rarity_code=rarity_code,
                rarity_name=rarity_code,  # Vanguard uses the code as the canonical name
                numbering_scheme="unique_per_rarity",
                card_base_id=card_number,  # updated in post-crawl same-name grouping
                extra=extra,
            )

    # ------------------------------------------------------------------
    # Post-crawl: same-name grouping
    # ------------------------------------------------------------------

    @staticmethod
    def _update_name_groups(conn: duckdb.DuckDBPyConnection) -> int:
        """Detect cards sharing the same name but having different card_numbers.

        In Vanguard, DZ-BT13/001 (RRR) and DZ-BT13/SR01 (SR) are the same
        logical card. Group them by card_name and set card_base_id to the
        lexicographically first card_number in the group.

        Returns the number of multi-number groups found.
        """
        rows = conn.execute("""
            SELECT card_name,
                   array_agg(DISTINCT card_number ORDER BY card_number) AS nums
            FROM raw_official_cards
            WHERE tcg = 'vanguard'
            GROUP BY card_name
            HAVING count(DISTINCT card_number) > 1
        """).fetchall()

        for card_name, card_numbers in rows:
            canonical = card_numbers[0]
            conn.execute(
                "UPDATE raw_official_cards SET card_base_id = ? "
                "WHERE tcg = 'vanguard' AND card_name = ?",
                [canonical, card_name],
            )

        return len(rows)

    # ------------------------------------------------------------------
    # Full crawl orchestration
    # ------------------------------------------------------------------

    def run_full_crawl(self, db_path=None) -> None:
        """End-to-end crawl:
        1. Parse all expansions from the site.
        2. Persist new expansions to vanguard_sets.
        3. Crawl cards for each expansion not yet in raw_official_cards.
        4. Update card_base_id for same-name groups.
        """
        conn = get_connection(db_path or DB_PATH)
        init_schema(conn)
        init_vanguard_schema(conn)

        sets = list(self.crawl_sets())

        already_stored = {
            r[0] for r in conn.execute("SELECT set_code FROM vanguard_sets").fetchall()
        }
        new_sets = [s for s in self._sets if s.set_code not in already_stored]
        if new_sets:
            conn.executemany(
                """INSERT INTO vanguard_sets
                       (expansion_id, set_code, set_name, set_title, category, release_date)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [
                    (s.expansion_id, s.set_code, s.set_name, s.set_title, s.category, s.release_date)
                    for s in new_sets
                ],
            )
            logger.info("Stored %d new expansions", len(new_sets))

        already_crawled = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT set_code FROM raw_official_cards WHERE tcg = 'vanguard'"
            ).fetchall()
        }
        to_crawl = [s for s in self._sets if s.set_code not in already_crawled]
        logger.info(
            "%d/%d expansions already in DB, crawling %d",
            len(already_crawled), len(self._sets), len(to_crawl),
        )

        for vg_set in tqdm(to_crawl, desc="Sets", unit="set"):
            logger.info("Crawling %s — %s", vg_set.set_code, vg_set.set_name)
            batch: list[dict] = []
            count = 0

            for card in self.crawl_cards(vg_set.set_code):
                batch.append({
                    "tcg": card.tcg,
                    "set_code": card.set_code,
                    "set_name": card.set_name,
                    "card_number": card.card_number,
                    "card_name": card.card_name,
                    "rarity_code": card.rarity_code,
                    "rarity_name": card.rarity_name,
                    "numbering_scheme": card.numbering_scheme,
                    "card_base_id": card.card_base_id,
                    "extra": json.dumps(card.extra, ensure_ascii=False),
                })
                count += 1
                if len(batch) >= 100:
                    insert_official_cards(conn, batch)
                    batch.clear()

            if batch:
                insert_official_cards(conn, batch)

            conn.execute(
                "UPDATE vanguard_sets SET total_cards = ?, crawled_at = now() WHERE set_code = ?",
                [count, vg_set.set_code],
            )
            logger.info("  → saved %d card editions", count)

        n_groups = self._update_name_groups(conn)
        logger.info("Same-name groups updated: %d", n_groups)

        conn.close()
        logger.info("Full crawl complete")


def init_vanguard_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create Vanguard-specific metadata table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vanguard_sets (
            expansion_id  INTEGER PRIMARY KEY,
            set_code      VARCHAR NOT NULL,
            set_name      VARCHAR,
            set_title     VARCHAR,
            category      VARCHAR,
            release_date  VARCHAR,
            total_cards   INTEGER,
            crawled_at    TIMESTAMPTZ
        );
    """)
