"""Z/X -Zillions of enemy X- official card list crawler.

Crawl procedure
---------------
1. GET https://www.zxtcg.com/card/ → parse all sets (secProduct) and
   rarities (secRarity) from the search-form dropdowns.
2. Check DuckDB: store any sets/rarities not yet recorded.
3. For each uncrawled set, paginate card listings (30 cards/page).
4. Parse every <section id="SETCODE-NNN-SS"> element:
     NNN  = 3-digit card number within the set
     SS   = 2-digit art index (00 = base art, 01+ = alternate art)
   Alternate arts share the same card_number; their rarity gets an "H"
   suffix (e.g. R → RH) following the community/shop convention.
5. After crawling all sets, detect cards that share the same name but
   appear under different card numbers (reprints) and record those groups.

HTML structure (confirmed from live page)
-----------------------------------------
<section id="B01-001-00" data-modal-card="B01-001-00">
  <div class="pic">
    <div class="rarity"><img alt="R" src="/assets/icon_img/r_r_sp.png"/></div>
    <div><img alt="" src="/assets/card_img/B01/B01-001.png"/></div>
  </div>
  <div class="desc">
    <h1>
      <span class="cardno">B01-001</span>
      <span class="name">運命の猟犬ライラプス</span>
    </h1>
    <ul class="icons">
      <li><img alt="赤" src="/assets/icon_img/w_red_sp.png"/></li>
      <li><img alt="イグニッション" src="/assets/icon_img/i_ignition_sp.png"/></li>
      <li><img alt="自動能力" src="/assets/icon_img/a_auto_sp.png"/></li>
    </ul>
    <p class="summary">ゼクス ／ <span class="iblock">ミソス</span></p>
    <p class="illustrator noteColor">Illustrator.華潤</p>
  </div>
</section>
"""

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Iterator

import duckdb
import requests
from bs4 import BeautifulSoup, Tag

from crawlers.official.base import OfficialCard, OfficialCrawler
from crawlers.storage import (
    DB_PATH,
    get_connection,
    init_schema,
    init_zx_schema,
    insert_official_cards,
)

logger = logging.getLogger(__name__)

ZX_BASE = "https://www.zxtcg.com"
CARD_LIST_URL = f"{ZX_BASE}/card/"
PAGE_SIZE = 30

# Parameters that control which card fields are shown in the listing.
DISPLAY_PARAMS = {"fwcn": "1", "fwil": "1", "fwct": "1", "fwft": "1", "fwrm": "1"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,zh-CN;q=0.9,zh;q=0.8",
}

# Regex matching a card section id: anything ending in a 2-digit art index
_SECTION_ID_RE = re.compile(r"^(.+)-(\d{2})$")


@dataclass
class ZXSet:
    pn_param: str        # dropdown param name, e.g. "pn1"
    set_full_value: str  # option value as-is, e.g. "B01　異世界との邂逅"
    set_code: str        # e.g. "B01"
    set_name: str        # e.g. "異世界との邂逅"


class ZXOfficialCrawler(OfficialCrawler):
    """Crawler for the Z/X official card database at zxtcg.com."""

    tcg = "zx"

    def __init__(self, delay: float = 1.5):
        """
        Args:
            delay: Seconds to sleep between HTTP requests (be polite).
        """
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._sets: list[ZXSet] = []
        self._rarities: list[tuple[str, str]] = []  # (rarity_code, rr_param)

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
    # Metadata: sets and rarities
    # ------------------------------------------------------------------

    def crawl_metadata(self) -> tuple[list[ZXSet], list[tuple[str, str]]]:
        """Fetch the card search page and parse all sets and rarities from dropdowns."""
        logger.info("Fetching metadata from %s", CARD_LIST_URL)
        soup = self._get(CARD_LIST_URL)

        # --- Rarities (secRarity section) ---
        rarities: list[tuple[str, str]] = []
        rarity_section = soup.find("section", class_="secRarity")
        if rarity_section:
            for select in rarity_section.find_all("select"):
                rr_param = select.get("name", "")
                for opt in select.find_all("option"):
                    code = opt.get("value", "").strip()
                    if code:
                        rarities.append((code, rr_param))

        # --- Sets (secProduct section) ---
        sets: list[ZXSet] = []
        product_section = soup.find("section", class_="secProduct")
        if product_section:
            for select in product_section.find_all("select"):
                pn_param = select.get("name", "")
                for opt in select.find_all("option"):
                    full_val = opt.get("value", "").strip()
                    if not full_val:
                        continue
                    # Values use a full-width space (U+3000) between code and name,
                    # e.g. "B01　異世界との邂逅" or "F01 F02　紅蓮の英雄＆漆黒の魔人"
                    parts = full_val.split("\u3000", maxsplit=1)
                    set_code = parts[0].strip()
                    set_name = parts[1].strip() if len(parts) > 1 else ""
                    sets.append(ZXSet(
                        pn_param=pn_param,
                        set_full_value=full_val,
                        set_code=set_code,
                        set_name=set_name,
                    ))

        self._sets = sets
        self._rarities = rarities
        logger.info("Found %d sets, %d rarities", len(sets), len(rarities))
        return sets, rarities

    def crawl_sets(self) -> Iterator[dict]:
        if not self._sets:
            self.crawl_metadata()
        for s in self._sets:
            yield {
                "set_code": s.set_code,
                "set_name": s.set_name,
                "set_full_value": s.set_full_value,
                "pn_param": s.pn_param,
            }

    # ------------------------------------------------------------------
    # Card parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _total_count(soup: BeautifulSoup) -> int:
        """Extract total card count from <div class="countRule">N～M件目 (全K件)</div>."""
        div = soup.find("div", class_="countRule")
        if not div:
            return 0
        m = re.search(r"全(\d+)件", div.get_text())
        return int(m.group(1)) if m else 0

    @staticmethod
    def _parse_section(section: Tag, set_info: ZXSet) -> OfficialCard | None:
        """Parse one <section id="SETCODE-NNN-SS"> into an OfficialCard.

        Fields extracted:
          cardno      → <span class="cardno">
          name        → <span class="name">
          rarity      → <div class="rarity"><img alt="R">  (alt = rarity code)
          color       → <ul class="icons"> first <img src="*w_*"> (alt = color name)
          card_type   → <p class="summary"> text before ／
          tribe       → <p class="summary"> text after ／
          illustrator → <p class="illustrator"> text after "Illustrator."
          abilities   → <ul class="icons"> all img alts whose src doesn't contain "w_"
        """
        section_id = section.get("id", "")
        m = _SECTION_ID_RE.match(section_id)
        if not m:
            return None

        card_number = m.group(1)   # e.g. "B01-001"
        art_index = m.group(2)     # e.g. "00"
        is_alt_art = art_index != "00"

        # Card number (also in <span class="cardno">, but id is authoritative)
        cardno_el = section.find("span", class_="cardno")
        # Use span if available (it matches id base), else derive from section_id
        card_number = cardno_el.get_text(strip=True) if cardno_el else card_number

        # Card name
        name_el = section.find("span", class_="name")
        if not name_el:
            logger.debug("No name element in section %s — skipping", section_id)
            return None
        card_name = name_el.get_text(strip=True)

        # Rarity: <div class="rarity"><img alt="R"/></div>
        rarity_div = section.find("div", class_="rarity")
        rarity_raw = ""
        if rarity_div:
            img = rarity_div.find("img")
            if img:
                rarity_raw = img.get("alt", "").strip()
        # Alternate arts get "H" appended: R → RH, SR → SRH
        rarity_code = f"{rarity_raw}H" if is_alt_art and rarity_raw else rarity_raw

        # Icons list: first img with src containing "w_" is the color;
        # remaining imgs (ignition, auto, etc.) are abilities.
        color = ""
        abilities: list[str] = []
        icons_ul = section.find("ul", class_="icons")
        if icons_ul:
            for img in icons_ul.find_all("img"):
                src = img.get("src", "")
                alt = img.get("alt", "").strip()
                if "/w_" in src:
                    if not color:          # first color icon wins
                        color = alt
                elif alt:
                    abilities.append(alt)

        # Card type / tribe from <p class="summary">ゼクス ／ <span>ミソス</span></p>
        card_type = ""
        tribe = ""
        summary_el = section.find("p", class_="summary")
        if summary_el:
            summary_text = summary_el.get_text(" ", strip=True)
            parts = summary_text.split("／", maxsplit=1)
            card_type = parts[0].strip()
            tribe = parts[1].strip() if len(parts) > 1 else ""

        # Illustrator from <p class="illustrator noteColor">Illustrator.華潤</p>
        illustrator = ""
        illust_el = section.find("p", class_="illustrator")
        if illust_el:
            illust_text = illust_el.get_text(strip=True)
            illustrator = illust_text.removeprefix("Illustrator.").strip()

        extra: dict = {
            "section_id": section_id,
            "art_index": art_index,
            "is_alt_art": is_alt_art,
            "color": color,
            "card_type": card_type,
            "tribe": tribe,
            "illustrator": illustrator,
            "abilities": abilities,
        }

        return OfficialCard(
            tcg="zx",
            set_code=set_info.set_code,
            set_name=set_info.set_name,
            card_number=card_number,
            card_name=card_name,
            rarity_code=rarity_code,
            rarity_name=rarity_code,          # ZX uses the code as the canonical name
            numbering_scheme="shared_official",
            card_base_id=card_number,         # alt arts share the base card's number
            extra=extra,
        )

    def crawl_cards(self, set_code: str) -> Iterator[OfficialCard]:
        """Yield all OfficialCard records for a set, paginating as needed."""
        if not self._sets:
            self.crawl_metadata()

        set_info = next((s for s in self._sets if s.set_code == set_code), None)
        if not set_info:
            raise ValueError(
                f"Unknown set code {set_code!r}. Call crawl_metadata() first."
            )

        base_params = {**DISPLAY_PARAMS, set_info.pn_param: set_info.set_full_value}

        soup = self._get(CARD_LIST_URL, params={**base_params, "page": "1"})
        total = self._total_count(soup)
        if total == 0:
            logger.warning("No cards found for set %s — possible empty set or site change", set_code)
            return

        total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
        logger.info("Set %s (%s): %d cards, %d pages", set_code, set_info.set_name, total, total_pages)

        def _iter_page(page_soup: BeautifulSoup) -> Iterator[OfficialCard]:
            for sec in page_soup.find_all("section", id=_SECTION_ID_RE):
                card = self._parse_section(sec, set_info)
                if card:
                    yield card

        yield from _iter_page(soup)

        for page_num in range(2, total_pages + 1):
            logger.debug("  page %d/%d", page_num, total_pages)
            page_soup = self._get(CARD_LIST_URL, params={**base_params, "page": str(page_num)})
            yield from _iter_page(page_soup)

    def crawl_all(self) -> Iterator[OfficialCard]:
        if not self._sets:
            self.crawl_metadata()
        for s in self._sets:
            yield from self.crawl_cards(s.set_code)

    # ------------------------------------------------------------------
    # Cross-set same-name grouping
    # ------------------------------------------------------------------

    @staticmethod
    def _update_name_groups(conn: duckdb.DuckDBPyConnection) -> int:
        """Detect cards sharing the same name across different card numbers (reprints).

        For each such group:
          - canonical_number = lexicographically first card_number (earliest set)
          - Updates raw_official_cards.card_base_id to canonical_number
          - Upserts into zx_card_name_groups

        Returns the number of multi-number groups found.
        """
        rows = conn.execute("""
            SELECT card_name,
                   array_agg(DISTINCT card_number ORDER BY card_number) AS nums
            FROM raw_official_cards
            WHERE tcg = 'zx'
            GROUP BY card_name
            HAVING count(DISTINCT card_number) > 1
        """).fetchall()

        if not rows:
            return 0

        for card_name, card_numbers in rows:
            canonical = card_numbers[0]  # lexicographically first = earliest set code
            conn.execute(
                "UPDATE raw_official_cards SET card_base_id = ? WHERE tcg = 'zx' AND card_name = ?",
                [canonical, card_name],
            )
            conn.execute(
                """INSERT OR REPLACE INTO zx_card_name_groups
                       (card_name, canonical_number, card_numbers)
                   VALUES (?, ?, ?)""",
                [card_name, canonical, json.dumps(card_numbers, ensure_ascii=False)],
            )

        return len(rows)

    # ------------------------------------------------------------------
    # Full crawl orchestration
    # ------------------------------------------------------------------

    def run_full_crawl(self, db_path=None) -> None:
        """
        End-to-end crawl:
          1. Parse sets and rarities from the search page.
          2. Persist any new sets/rarities to DuckDB.
          3. Crawl cards for each set not yet in the DB.
          4. Compute and store cross-set same-name (reprint) groups.
        """
        conn = get_connection(db_path or DB_PATH)
        init_schema(conn)
        init_zx_schema(conn)

        # ---- 1. Metadata ----
        sets, rarities = self.crawl_metadata()

        existing_sets = {
            r[0] for r in conn.execute("SELECT set_code FROM zx_sets").fetchall()
        }
        existing_rarities = {
            r[0] for r in conn.execute("SELECT rarity_code FROM zx_rarities").fetchall()
        }

        new_sets = [s for s in sets if s.set_code not in existing_sets]
        new_rarities = [
            (code, param) for code, param in rarities if code not in existing_rarities
        ]

        # ---- 2. Persist metadata ----
        if new_sets:
            conn.executemany(
                "INSERT INTO zx_sets (set_code, set_name, set_full_value, pn_param) VALUES (?, ?, ?, ?)",
                [(s.set_code, s.set_name, s.set_full_value, s.pn_param) for s in new_sets],
            )
            logger.info("Stored %d new sets", len(new_sets))

        if new_rarities:
            conn.executemany(
                "INSERT INTO zx_rarities (rarity_code, rr_param) VALUES (?, ?)",
                new_rarities,
            )
            logger.info("Stored %d new rarities", len(new_rarities))

        # ---- 3. Crawl cards ----
        already_crawled = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT set_code FROM raw_official_cards WHERE tcg = 'zx'"
            ).fetchall()
        }
        to_crawl = [s for s in sets if s.set_code not in already_crawled]
        logger.info(
            "%d/%d sets already in DB, crawling %d",
            len(already_crawled), len(sets), len(to_crawl),
        )

        for set_info in to_crawl:
            logger.info("Crawling %s — %s", set_info.set_code, set_info.set_name)
            batch: list[dict] = []
            count = 0

            for card in self.crawl_cards(set_info.set_code):
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
                "UPDATE zx_sets SET total_cards = ?, crawled_at = now() WHERE set_code = ?",
                [count, set_info.set_code],
            )
            logger.info("  → saved %d card editions", count)

        # ---- 4. Cross-set same-name grouping ----
        n_groups = self._update_name_groups(conn)
        logger.info("Cross-set reprint groups found: %d", n_groups)

        conn.close()
        logger.info("Full crawl complete")
