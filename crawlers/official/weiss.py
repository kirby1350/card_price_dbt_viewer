"""Weiss Schwarz official card list crawler — ws-tcg.com (Japanese).

API structure (the cardlist page is now a JS app backed by a JSON API)
----------------------------------------------------------------------
  Filter options : GET  https://ws-tcg.com/manage/CardListUser/filter-options
                   → JSON with `expansions` (id, name, …), `cardKinds`, etc.
  Card search    : GET  https://ws-tcg.com/manage/CardListUser/searchJson
                   params: expansion=NNN&page=N
                   → JSON { items: [...], total, page, limit, page_count }
                   `limit` is fixed server-side (25); paginate via `page_count`.

searchJson item fields
----------------------
  card_number   "DDD/S129-001"        rare        "RR"
  card_name     "呪いの衝突 オカルン"   card_kind   "2"  (code → name via cardKinds)
  level / cost / power                  side        "-2"
  color         "[[yellow.gif]]"        soul        "[[soul.gif]]"
  card_trigger  "-"                     parallel_param "〇" for parallels/SP
  feature1/2/3  trait fragments         text        rules text
  flavor                                picture     "d/ddd_s129/ddd_s129_001.png"

Key design decisions
---------------------
  numbering_scheme : "unique_per_rarity" — SP/parallel variants carry a distinct
                     card number suffix (e.g. DDD/S129-001 RR, -001S SR, -001SSP SSP
                     are separate items with their own card_number).
  card_base_id     : card_number with any trailing letter suffix stripped, so
                     DDD/S129-001S groups back to DDD/S129-001.
  set_code         : prefix before the dash: "DDD/S129-001" → "DDD/S129".
  image_url        : IMAGE_BASE + `picture`.
  All card data is available on the search results JSON; no per-card detail fetches.
  Expansion IDs are passed as set_code when using --set (e.g. --set 551).
"""

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Iterator

import requests
from tqdm import tqdm

from crawlers.official.base import OfficialCard, OfficialCrawler
from crawlers.storage import DB_PATH, get_connection, init_schema, insert_official_cards

logger = logging.getLogger(__name__)

WS_BASE = "https://ws-tcg.com"
API_BASE = f"{WS_BASE}/manage/CardListUser"
FILTER_OPTIONS_URL = f"{API_BASE}/filter-options"
SEARCH_JSON_URL = f"{API_BASE}/searchJson"
IMAGE_BASE = f"{WS_BASE}/wordpress/wp-content/images/cardlist/"

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "ja,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}

# Strip trailing non-digit suffix from the number portion
_BASE_ID_RE = re.compile(r"^(.*-\d+)[A-Za-z]+$")
# Image-token markup the API uses for color/soul, e.g. "[[yellow.gif]]"
_IMG_TOKEN_RE = re.compile(r"\[\[([^\]]+?)\]\]")


@dataclass
class WSExpansion:
    expansion_id: int
    set_name: str


def _init_weiss_schema(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weiss_sets (
            expansion_id  INTEGER PRIMARY KEY,
            set_code      VARCHAR,
            set_name      VARCHAR,
            total_cards   INTEGER,
            crawled_at    TIMESTAMPTZ
        );
    """)


def _extract_set_code(card_number: str) -> str:
    """'IM/S07-001' → 'IM/S07'."""
    idx = card_number.rfind("-")
    return card_number[:idx] if idx != -1 else card_number


def _card_base_id(card_number: str) -> str:
    """'IM/S07-001S' → 'IM/S07-001', 'IM/S07-001' → 'IM/S07-001'."""
    m = _BASE_ID_RE.match(card_number)
    return m.group(1) if m else card_number


def _clean_markup(value: str) -> str:
    """Reduce image-token markup to filename stems.

    '[[yellow.gif]]'           → 'yellow'
    '[[soul.gif]][[soul.gif]]' → 'soul,soul'
    plain text                 → returned stripped
    """
    tokens = _IMG_TOKEN_RE.findall(value or "")
    if tokens:
        return ",".join(t.rsplit(".", 1)[0] for t in tokens)
    return (value or "").strip()


def _join_traits(*features: str) -> str:
    """Combine feature1/2/3 into '音楽・アイドル', dropping blanks and '-'."""
    parts = [f.strip() for f in features if f and f.strip() and f.strip() != "-"]
    return "・".join(parts)


class WeissOfficialCrawler(OfficialCrawler):
    """Official card crawler for the Japanese Weiss Schwarz card database."""

    tcg = "weiss"

    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._expansions: list[WSExpansion] = []
        # card_kind code (e.g. "2") → display name (e.g. "キャラ")
        self._card_kind_map: dict[str, str] = {}

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get_json(self, url: str, params: dict | None = None) -> dict:
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        time.sleep(self.delay)
        return resp.json()

    # ------------------------------------------------------------------
    # Expansion discovery
    # ------------------------------------------------------------------

    def crawl_sets(self) -> Iterator[WSExpansion]:
        if not self._expansions:
            self._expansions = self._fetch_expansions()
        yield from self._expansions

    def _fetch_expansions(self) -> list[WSExpansion]:
        logger.info("Fetching Weiss Schwarz expansion list")
        data = self._get_json(FILTER_OPTIONS_URL)

        # Cache card-kind code → name lookup for card parsing.
        self._card_kind_map = {
            str(k.get("value", "")): k.get("name", "")
            for k in data.get("cardKinds", [])
        }

        expansions = []
        for e in data.get("expansions", []):
            if str(e.get("disp_flg", "1")) in ("0", "False", "false"):
                continue
            name = (e.get("name") or "").strip()
            try:
                exp_id = int(e["id"])
            except (KeyError, TypeError, ValueError):
                continue
            expansions.append(WSExpansion(expansion_id=exp_id, set_name=name))
        logger.info("Found %d expansions", len(expansions))
        return expansions

    # ------------------------------------------------------------------
    # OfficialCrawler interface
    # ------------------------------------------------------------------

    def crawl_cards(self, set_code: str) -> Iterator[OfficialCard]:
        """Crawl cards for an expansion. Pass expansion_id as set_code (e.g. '551')."""
        list(self.crawl_sets())
        if not set_code.isdigit():
            logger.error(
                "weiss-official --set requires a numeric expansion_id (e.g. --set 551)"
            )
            return
        matched = [e for e in self._expansions if e.expansion_id == int(set_code)]
        if not matched:
            logger.warning("Expansion id %s not found", set_code)
            return
        yield from self._crawl_expansion(matched[0])

    def _fetch_expansion_items(self, expansion_id: int) -> list[dict]:
        """Fetch all card items for an expansion, paginating via page_count."""
        items: list[dict] = []
        page = 1
        while True:
            data = self._get_json(
                SEARCH_JSON_URL, params={"expansion": str(expansion_id), "page": page}
            )
            page_items = data.get("items", [])
            items.extend(page_items)
            page_count = data.get("page_count") or 1
            if page >= page_count or not page_items:
                break
            page += 1
        return items

    def _crawl_expansion(self, exp: WSExpansion) -> Iterator[OfficialCard]:
        logger.info("Crawling expansion %d — %s", exp.expansion_id, exp.set_name)

        items = self._fetch_expansion_items(exp.expansion_id)
        logger.info("  %d cards found", len(items))

        for item in tqdm(items, desc=exp.set_name[:40], unit="card", leave=False):
            card_number = (item.get("card_number") or "").strip()
            if not card_number:
                continue

            rarity = (item.get("rare") or "").strip()
            picture = (item.get("picture") or "").strip()
            image_url = f"{IMAGE_BASE}{picture}" if picture else ""
            card_kind_code = str(item.get("card_kind", ""))

            yield OfficialCard(
                tcg=self.tcg,
                set_code=_extract_set_code(card_number),
                set_name=exp.set_name,
                card_number=card_number,
                card_name=(item.get("card_name") or "").strip(),
                rarity_code=rarity,
                rarity_name=rarity,
                numbering_scheme="unique_per_rarity",
                card_base_id=_card_base_id(card_number),
                image_url=image_url,
                extra={
                    "expansion_id": exp.expansion_id,
                    "side": str(item.get("side", "")),
                    "card_type": self._card_kind_map.get(card_kind_code, card_kind_code),
                    "level": str(item.get("level", "")),
                    "color": _clean_markup(item.get("color", "")),
                    "power": str(item.get("power", "")),
                    "soul": _clean_markup(item.get("soul", "")),
                    "cost": str(item.get("cost", "")),
                    "trigger": _clean_markup(item.get("card_trigger", "")),
                    "traits": _join_traits(
                        item.get("feature1", ""),
                        item.get("feature2", ""),
                        item.get("feature3", ""),
                    ),
                    "flavor": (item.get("flavor") or "").strip(),
                    "effect": (item.get("text") or "").strip(),
                    "parallel": item.get("parallel_param", ""),
                    "image_url": image_url,
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
        _init_weiss_schema(conn)

        all_expansions = self._fetch_expansions()
        if not all_expansions:
            logger.error("No expansions found — check network connection")
            return

        done_ids: set[int] = {
            r[0] for r in conn.execute(
                "SELECT expansion_id FROM weiss_sets WHERE crawled_at IS NOT NULL"
            ).fetchall()
        }

        to_crawl = [e for e in all_expansions if e.expansion_id not in done_ids]
        logger.info(
            "%d/%d expansions already crawled, crawling %d remaining",
            len(done_ids), len(all_expansions), len(to_crawl),
        )

        for exp in tqdm(to_crawl, desc="Expansions", unit="set"):
            batch: list[dict] = []
            set_code_found = None
            count = 0

            try:
                for card in self._crawl_expansion(exp):
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
                    if len(batch) >= 200:
                        insert_official_cards(conn, batch)
                        batch.clear()
            except Exception:
                logger.exception("Failed to crawl expansion %d — skipping", exp.expansion_id)
                continue

            if batch:
                insert_official_cards(conn, batch)

            conn.execute(
                """INSERT OR REPLACE INTO weiss_sets
                       (expansion_id, set_code, set_name, total_cards, crawled_at)
                   VALUES (?, ?, ?, ?, now())""",
                [exp.expansion_id, set_code_found, exp.set_name, count],
            )
            logger.info("  saved %d cards for expansion %d", count, exp.expansion_id)

        if _own_conn:
            conn.close()
        logger.info("Weiss Schwarz full crawl complete")
