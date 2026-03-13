"""Union Arena Simplified Chinese translation crawler — unionarena-tcg.cn.

Source
------
The CN site uses a JSON API at uapcapi.windoent.com.  No authentication
is required for the endpoints we use.

API endpoints
-------------
  Set list  : GET https://uapcapi.windoent.com/card/card/attrweblist
              → data.goods[]: {id, name}  (name contains 【SET_CODE】)

  Card list : GET https://uapcapi.windoent.com/card/card/weblist
              params: good={set_name}, page={n}, limit=100
              → page.list[]: {id, image}

  Card detail: GET https://uapcapi.windoent.com/card/card/webinfo/{id}
              → data: {number, name, works, rarity, cardType, bp, ...}
              number uses the same format as the JP site: "UA01BT/CGH-1-001"

What is stored
--------------
  raw_card_translations (tcg=unionarena, language=zh-CN):
      card_number  → matches raw_official_cards.card_number exactly
      card_name    → Simplified Chinese card name

  raw_set_translations (tcg=unionarena, language=zh-CN):
      set_code     → e.g. "UA01BT"
      set_name     → Simplified Chinese title (stripped of 【...】 suffix)
"""

import logging
import re
import time
from dataclasses import dataclass

import requests

from crawlers.storage import (
    DB_PATH,
    get_connection,
    init_schema,
    insert_card_translations,
    insert_set_translations,
)

logger = logging.getLogger(__name__)

API_BASE = "https://uapcapi.windoent.com"
HEADERS = {
    "Accept": "application/json",
    "Origin": "https://www.unionarena-tcg.cn",
    "Referer": "https://www.unionarena-tcg.cn/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}

_SET_CODE_RE = re.compile(r"【([^】]+)】")
TCG = "unionarena"
LANGUAGE = "zh-CN"


@dataclass
class CNSet:
    good_name: str   # full string as returned by API, e.g. "CODE GEASS 反叛的鲁路修 补充包 【UA01BT】"
    set_code: str    # e.g. "UA01BT"
    set_name: str    # Chinese title without bracket suffix, e.g. "CODE GEASS 反叛的鲁路修 补充包"


class UnionArenaCNTranslationCrawler:
    """Fetches Simplified Chinese card/set names from the CN Union Arena site
    and stores them as translations linked to the existing JP card data."""

    def __init__(self, delay: float = 0.5):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> dict:
        resp = self.session.get(url, params=params, timeout=20)
        resp.raise_for_status()
        time.sleep(self.delay)
        return resp.json()

    # ------------------------------------------------------------------
    # Set discovery
    # ------------------------------------------------------------------

    def fetch_sets(self) -> list[CNSet]:
        """Return all sets that have a 【SET_CODE】 bracket in their name."""
        data = self._get(f"{API_BASE}/card/card/attrweblist")
        goods = data.get("data", {}).get("goods", [])
        sets = []
        for g in goods:
            name = g.get("name", "")
            m = _SET_CODE_RE.search(name)
            if not m:
                continue
            set_code = m.group(1).strip()
            # Strip the 【...】 suffix and trailing whitespace for the display name
            set_name = _SET_CODE_RE.sub("", name).strip()
            sets.append(CNSet(good_name=name, set_code=set_code, set_name=set_name))
        logger.info("CN site: found %d sets with set codes", len(sets))
        return sets

    # ------------------------------------------------------------------
    # Card id enumeration per set
    # ------------------------------------------------------------------

    def _fetch_card_ids(self, cn_set: CNSet) -> list[int]:
        """Return all card internal IDs for a set."""
        ids = []
        page = 1
        while True:
            data = self._get(
                f"{API_BASE}/card/card/weblist",
                params={"good": cn_set.good_name, "page": page, "limit": 100},
            )
            page_data = data.get("page", {})
            items = page_data.get("list", [])
            if not items:
                break
            ids.extend(item["id"] for item in items)
            total_pages = page_data.get("totalPage", 1)
            if page >= total_pages:
                break
            page += 1
        return ids

    # ------------------------------------------------------------------
    # Card detail
    # ------------------------------------------------------------------

    def _fetch_card_detail(self, card_id: int) -> dict | None:
        """Fetch Chinese card detail. Returns None on error."""
        try:
            data = self._get(f"{API_BASE}/card/card/webinfo/{card_id}")
            return data.get("data")
        except Exception:
            logger.warning("Failed to fetch CN detail for id=%d", card_id)
            return None

    # ------------------------------------------------------------------
    # Full crawl
    # ------------------------------------------------------------------

    def run_full_crawl(self, db_path=None, conn=None) -> None:
        """Crawl all CN sets and persist translations.

        Args:
            db_path: DuckDB file path (default: data/raw.duckdb).
            conn:    Pre-opened connection (DuckDB or PgAdapter). When provided,
                     db_path is ignored and the caller is responsible for closing.
        """
        _own_conn = conn is None
        if _own_conn:
            conn = get_connection(db_path or DB_PATH)
        init_schema(conn)

        all_sets = self.fetch_sets()
        if not all_sets:
            logger.error("No CN sets found — check network")
            return

        # Find which set_codes already have translations
        done_codes: set[str] = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT set_code FROM raw_set_translations "
                "WHERE tcg = ? AND language = ?",
                [TCG, LANGUAGE],
            ).fetchall()
        }

        to_crawl = [s for s in all_sets if s.set_code not in done_codes]
        logger.info(
            "%d/%d sets already translated, crawling %d remaining",
            len(done_codes), len(all_sets), len(to_crawl),
        )

        for cn_set in to_crawl:
            logger.info("→ %s  %s", cn_set.set_code, cn_set.set_name)

            # Persist set translation immediately
            insert_set_translations(conn, [{
                "tcg": TCG,
                "set_code": cn_set.set_code,
                "language": LANGUAGE,
                "set_name": cn_set.set_name,
            }])

            # Enumerate all card IDs in this set
            card_ids = self._fetch_card_ids(cn_set)
            logger.info("  %d cards in set", len(card_ids))

            card_batch: list[dict] = []
            saved = 0

            for card_id in card_ids:
                detail = self._fetch_card_detail(card_id)
                if not detail:
                    continue

                card_number = detail.get("number", "").strip()
                card_name = detail.get("name", "").strip()
                if not card_number or not card_name:
                    logger.debug("  Skipping id=%d — missing number or name", card_id)
                    continue

                card_batch.append({
                    "tcg": TCG,
                    "card_number": card_number,
                    "language": LANGUAGE,
                    "card_name": card_name,
                })
                saved += 1

                if len(card_batch) >= 100:
                    insert_card_translations(conn, card_batch)
                    card_batch.clear()

            if card_batch:
                insert_card_translations(conn, card_batch)

            logger.info("  saved %d card translations for %s", saved, cn_set.set_code)

        if _own_conn:
            conn.close()
        logger.info("Union Arena CN translation crawl complete")
