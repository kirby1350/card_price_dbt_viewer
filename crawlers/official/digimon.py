"""Digimon Card Game official card list crawler — digimoncard.com (Japanese).

URL structure
-------------
  Entry point    : GET  https://digimoncard.com/cards/
                   → 302 → /cards/?search=true&category=<latest_id>
  Card search    : GET  https://digimoncard.com/cards/?search=true&category=NNN
                   All cards for a category are embedded in one HTML response;
                   JavaScript handles page-switching client-side with CSS classes
                   (page-1, page-2, …) — no HTTP pagination required.
  Categories     : Listed in <select name="category"> on the search page.

HTML structure — card list (ul.image_lists > li.image_lists_item)
-----------------------------------------------------------------
  <li class="image_lists_item data page-1">
    <a class="card_img" data-src="#EX11-001" href="javascript:void(0);">
      <img src="../images/cardlist/card/EX11-001.png?02"/>
    </a>
    <div class="popupCol" id="EX11-001">
      <div class="cardDetailCol">
        <ul class="cardTitleList">
          <li class="cardNo">EX11-001</li>
          <li class="cardRarity">C</li>
          <li class="cardType">デジモン</li>
          <li class="cardLv">Lv.2</li>
        </ul>
        <div class="cardTitle">アグモン</div>
        <div class="cardImg">
          <img src="../images/cardlist/card/EX11-001.png?02"/>
        </div>
        <div class="cardInfoCol">
          <dl class="cardInfoBox col2">
            <dt class="cardInfoTit">色</dt>
            <dd class="cardInfoData cardColor">
              <span class="cardColor_red">赤</span>
            </dd>
          </dl>
          <dl class="cardInfoBox col2">
            <dt>形態</dt><dd>幼年期I</dd>
          </dl>
          <dl class="cardInfoBox col2">
            <dt>タイプ</dt><dd>爬虫類型/爬竜型</dd>
          </dl>
          <div class="cardInfoBox">
            <dl class="cardInfoBoxSmall">
              <dt>[固有スキル]</dt><dd>…</dd>
            </dl>
          </div>
          <dl class="cardInfoBox">
            <dt>製品情報</dt>
            <dd>… DAWN OF LIBERATOR【EX-11】…</dd>
          </dl>
        </div>
      </div>
    </div>
  </li>

Parallel variants have a `_P1` / `_P2` suffix in `data-src` and popup `id`:
  <a class="card_img" data-src="#EX11-001_P1" …>

Key design decisions
---------------------
  numbering_scheme : "shared_official" — same official card_number across normal and
                     parallel printings (e.g. EX11-001).  Parallel variants are
                     distinguished by appending the suffix to rarity_code:
                     normal → "C", first parallel → "C_P1", second → "C_P2".
  card_base_id     : official card_number (without parallel suffix).
  set_code         : prefix before the dash: "EX11-001" → "EX11".
  No HTTP pagination — all cards for a category arrive in one response.
"""

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Iterator

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from crawlers.official.base import OfficialCard, OfficialCrawler
from crawlers.storage import DB_PATH, get_connection, init_schema, insert_official_cards

logger = logging.getLogger(__name__)

DIGIMON_BASE = "https://digimoncard.com"
CARDS_URL = f"{DIGIMON_BASE}/cards/"
SEARCH_URL = f"{DIGIMON_BASE}/cards/"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}

# EX11-001, BT24-001, ST18-04, P-001 etc.
_CARD_NO_RE = re.compile(r"^([A-Z0-9]+-[A-Z0-9]+)-\d+")
# Parallel suffix in data-src anchor id: EX11-001_P1 → suffix "_P1"
_PARALLEL_RE = re.compile(r"_P\d+$")


@dataclass
class DigimonSet:
    category_id: int
    set_name: str


def _init_digimon_schema(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS digimon_sets (
            category_id  INTEGER PRIMARY KEY,
            set_code     VARCHAR,
            set_name     VARCHAR,
            total_cards  INTEGER,
            crawled_at   TIMESTAMPTZ
        );
    """)


def _extract_set_code(card_number: str) -> str:
    """'EX11-001' → 'EX11', 'BT24-001' → 'BT24', 'ST18-04' → 'ST18'."""
    idx = card_number.rfind("-")
    return card_number[:idx] if idx != -1 else card_number


def _parse_card_li(li) -> dict | None:
    """Parse one <li class='image_lists_item'> into a dict of card fields."""
    anchor = li.find("a", class_="card_img")
    if not anchor:
        return None

    data_src = anchor.get("data-src", "").lstrip("#")
    if not data_src:
        return None

    # Parallel suffix detection
    p_match = _PARALLEL_RE.search(data_src)
    parallel_suffix = p_match.group(0) if p_match else ""
    card_number = data_src[: len(data_src) - len(parallel_suffix)] if parallel_suffix else data_src

    # Card detail is in the popup div
    popup = li.find("div", class_="popupCol")
    if not popup:
        return None

    # Basic fields from cardTitleList
    no_el = popup.find("li", class_="cardNo")
    official_number = no_el.get_text(strip=True) if no_el else card_number

    rar_el = popup.find("li", class_="cardRarity")
    rarity = rar_el.get_text(strip=True) if rar_el else ""

    type_el = popup.find("li", class_="cardType")
    card_type = type_el.get_text(strip=True) if type_el else ""

    lv_el = popup.find("li", class_="cardLv")
    level = lv_el.get_text(strip=True) if lv_el else ""

    name_el = popup.find("div", class_="cardTitle")
    card_name = name_el.get_text(strip=True) if name_el else ""

    # Color from span.cardColor_* class
    color_dd = popup.find("dd", class_="cardColor")
    colors = []
    if color_dd:
        for span in color_dd.find_all("span"):
            for cls in span.get("class", []):
                if cls.startswith("cardColor_") and len(cls) > len("cardColor_"):
                    colors.append(cls[len("cardColor_"):])

    # Image URL from popup cardImg div (more reliable than the thumbnail)
    image_url = ""
    card_img_div = popup.find("div", class_="cardImg")
    if card_img_div:
        img = card_img_div.find("img")
        if img:
            src = img.get("src", "")
            if src.startswith("../"):
                src = DIGIMON_BASE + "/cards/" + src[3:]
            elif src.startswith("/"):
                src = DIGIMON_BASE + src
            image_url = src

    # Parse dl key→value pairs (excluding FAQ dls which have very long dt text)
    kv: dict[str, str] = {}
    for dl in popup.find_all("dl"):
        dt = dl.find("dt")
        dd = dl.find("dd")
        if not dt or not dd:
            continue
        key = dt.get_text(strip=True)
        if len(key) > 30:  # skip FAQ question dts
            continue
        kv[key] = dd.get_text(strip=True)

    # Rarity code: append parallel suffix so each printing is unique
    rarity_code = rarity + parallel_suffix if parallel_suffix else rarity

    return {
        "card_number": official_number,
        "rarity_code": rarity_code,
        "rarity_name": rarity,
        "parallel_suffix": parallel_suffix,
        "card_name": card_name,
        "card_type": card_type,
        "level": level,
        "color": ",".join(colors),
        "form": kv.get("形態", ""),
        "attribute": kv.get("属性", ""),
        "card_subtype": kv.get("タイプ", ""),
        "dp": kv.get("DP", ""),
        "image_url": image_url,
    }


class DigimonOfficialCrawler(OfficialCrawler):
    """Official card crawler for the Japanese Digimon Card Game database."""

    tcg = "digimon"

    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._sets: list[DigimonSet] = []

    def _get(self, url: str, params: dict | None = None) -> BeautifulSoup:
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        time.sleep(self.delay)
        return BeautifulSoup(resp.text, "lxml")

    # ------------------------------------------------------------------
    # Set discovery
    # ------------------------------------------------------------------

    def crawl_sets(self) -> Iterator[DigimonSet]:
        if not self._sets:
            self._sets = self._fetch_sets()
        yield from self._sets

    def _fetch_sets(self) -> list[DigimonSet]:
        logger.info("Fetching Digimon set list")
        # Follow the redirect from /cards/ to get the search page with the category select
        soup = self._get(CARDS_URL, params={"search": "true", "category": ""})
        select = soup.find("select", {"name": "category"})
        if not select:
            logger.error("Could not find category select on %s", CARDS_URL)
            return []
        sets = []
        for opt in select.find_all("option"):
            val = opt.get("value", "").strip()
            if not val or not val.isdigit():
                continue
            sets.append(DigimonSet(
                category_id=int(val),
                set_name=opt.get_text(strip=True),
            ))
        logger.info("Found %d sets", len(sets))
        return sets

    # ------------------------------------------------------------------
    # OfficialCrawler interface
    # ------------------------------------------------------------------

    def crawl_cards(self, set_code: str) -> Iterator[OfficialCard]:
        """Crawl cards for a set. Pass category_id as set_code (e.g. '503036')."""
        list(self.crawl_sets())
        if not set_code.isdigit():
            logger.error(
                "digimon-official --set requires a numeric category_id (e.g. --set 503036)"
            )
            return
        matched = [s for s in self._sets if s.category_id == int(set_code)]
        if not matched:
            logger.warning("Category id %s not found", set_code)
            return
        yield from self._crawl_set(matched[0])

    def _crawl_set(self, dset: DigimonSet) -> Iterator[OfficialCard]:
        logger.info("Crawling category %d — %s", dset.category_id, dset.set_name)
        soup = self._get(SEARCH_URL, params={"search": "true", "category": str(dset.category_id)})

        items = soup.find_all("li", class_="image_lists_item")
        logger.info("  %d card entries found", len(items))

        for li in tqdm(items, desc=dset.set_name[:40], unit="card", leave=False):
            card = _parse_card_li(li)
            if not card:
                continue
            card_number = card["card_number"]
            set_code = _extract_set_code(card_number)
            yield OfficialCard(
                tcg=self.tcg,
                set_code=set_code,
                set_name=dset.set_name,
                card_number=card_number,
                card_name=card["card_name"],
                rarity_code=card["rarity_code"],
                rarity_name=card["rarity_name"],
                numbering_scheme="shared_official",
                card_base_id=card_number,
                image_url=card["image_url"],
                extra={
                    "category_id": dset.category_id,
                    "card_type": card["card_type"],
                    "level": card["level"],
                    "color": card["color"],
                    "form": card["form"],
                    "attribute": card["attribute"],
                    "card_subtype": card["card_subtype"],
                    "dp": card["dp"],
                    "parallel_suffix": card["parallel_suffix"],
                    "image_url": card["image_url"],
                },
            )

    # ------------------------------------------------------------------
    # Full crawl
    # ------------------------------------------------------------------

    def run_full_crawl(self, db_path=None, conn=None) -> None:
        _own_conn = conn is None
        if _own_conn:
            conn = get_connection(db_path or DB_PATH)
        init_schema(conn)
        _init_digimon_schema(conn)

        all_sets = self._fetch_sets()
        if not all_sets:
            logger.error("No sets found — check network connection")
            return

        done_ids: set[int] = {
            r[0] for r in conn.execute(
                "SELECT category_id FROM digimon_sets WHERE crawled_at IS NOT NULL"
            ).fetchall()
        }

        to_crawl = [s for s in all_sets if s.category_id not in done_ids]
        logger.info(
            "%d/%d sets already crawled, crawling %d remaining",
            len(done_ids), len(all_sets), len(to_crawl),
        )

        for dset in tqdm(to_crawl, desc="Sets", unit="set"):
            batch: list[dict] = []
            set_code_found = None
            count = 0

            try:
                for card in self._crawl_set(dset):
                    if set_code_found is None:
                        set_code_found = card.set_code
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
                        "image_url": card.image_url,
                        "extra": json.dumps(card.extra, ensure_ascii=False),
                    })
                    count += 1
                    if len(batch) >= 500:
                        insert_official_cards(conn, batch)
                        batch.clear()
            except Exception:
                logger.exception("Failed to crawl set %d — skipping", dset.category_id)
                continue

            if batch:
                insert_official_cards(conn, batch)

            conn.execute(
                """INSERT OR REPLACE INTO digimon_sets
                       (category_id, set_code, set_name, total_cards, crawled_at)
                   VALUES (?, ?, ?, ?, now())""",
                [dset.category_id, set_code_found, dset.set_name, count],
            )
            logger.info("  saved %d cards for set %d", count, dset.category_id)

        if _own_conn:
            conn.close()
        logger.info("Digimon full crawl complete")
