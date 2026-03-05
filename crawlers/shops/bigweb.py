"""Bigweb (bigweb.co.jp) shop crawler — API-based, generic across TCGs.

API endpoints
-------------
  Cardset list : GET https://api.bigweb.co.jp/cardsets?game_id={game_id}
  Products     : GET https://api.bigweb.co.jp/products?game_id={game_id}&cardsets={cardset_id}&page={n}

Known game_id / game_code pairs
---------------------------------
  151 / zx  → Z/X -Zillions of enemy X-
  (add more as needed; code comes from game.code in the API response)

Response structure (products endpoint)
----------------------------------------
  pagenate.count    — total items for the set
  pagenate.pageCount — number of pages (100 items/page)
  items[]
    id            — bigweb internal product ID
    name          — card name
    comment       — card number (may have trailing store notes, e.g. "B01-001 秋葉原店...")
    rarity.web    — rarity string; "-" for non-single items
    rarity.slip   — rarity short code
    cardset.id    — bigweb cardset ID
    cardset.slip  — set code (e.g. "IG08", "B01")
    cardset.web   — full set name
    price         — selling price (JPY integer)
    stock_count   — available quantity (0 = sold out)
    is_sold_out   — boolean
    is_box        — True for sealed products (skip these)
    card_condition — game-specific card category (Z/X card world); NOT physical condition

Filtering irrelevant items
---------------------------
  Skip when:  is_box=True  OR  rarity.web in ('', '-')  OR  comment is empty
  These cover: sealed boxes, non-card merchandise, items with no card number.
"""

import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Iterator

import requests

from crawlers.shops.base import ShopCrawler, ShopListing
from crawlers.storage import DB_PATH, get_connection, init_schema, insert_shop_listings

logger = logging.getLogger(__name__)

BIGWEB_API_BASE = "https://api.bigweb.co.jp"
BIGWEB_WEB_BASE = "https://www.bigweb.co.jp"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ja,zh-CN;q=0.9",
    "Origin": BIGWEB_WEB_BASE,
    "Referer": f"{BIGWEB_WEB_BASE}/",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    ),
}

# Card number: leading part of comment before any whitespace
_CARD_NUMBER_RE = re.compile(r"^\S+")


def _init_bigweb_schema(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bigweb_cardsets (
            game_id         INTEGER NOT NULL,
            cardset_id      INTEGER NOT NULL,
            set_code        VARCHAR,
            set_name        VARCHAR,
            last_crawled_at TIMESTAMPTZ,
            listing_count   INTEGER,
            PRIMARY KEY (game_id, cardset_id)
        );
    """)


class BigwebShopCrawler(ShopCrawler):
    """Shop price crawler for bigweb.co.jp using its JSON API.

    Args:
        game_id:   Bigweb internal game ID (e.g. 151 for Z/X)
        game_code: Bigweb URL game code (e.g. "zx")
        tcg:       Our canonical TCG identifier (e.g. "zx", "yugioh")
        delay:     Seconds to sleep between API requests
    """

    shop = "bigweb"

    def __init__(self, game_id: int, game_code: str, tcg: str, delay: float = 1.0):
        self.game_id = game_id
        self.game_code = game_code
        self.tcg = tcg
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._cardsets: list[dict] = []

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _get_json(self, url: str, params: dict | None = None) -> dict:
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        time.sleep(self.delay)
        return resp.json()

    # ------------------------------------------------------------------
    # Cardset (set) discovery
    # ------------------------------------------------------------------

    def fetch_sets(self) -> list[dict]:
        """Fetch all real cardsets for this game from the API."""
        if self._cardsets:
            return self._cardsets
        logger.info("Fetching cardset list from bigweb API (game_id=%s)", self.game_id)
        data = self._get_json(
            f"{BIGWEB_API_BASE}/cardsets",
            params={"game_id": self.game_id},
        )
        # Filter out separator rows (empty code) and special non-set entries
        self._cardsets = [
            s for s in data.get("cardsets", [])
            if s.get("code") and not self._is_irrelevant_cardset(s)
        ]
        logger.info("Found %d cardsets", len(self._cardsets))
        return self._cardsets

    @staticmethod
    def _is_irrelevant_cardset(cardset: dict) -> bool:
        """Skip cardsets that never contain standard single cards."""
        code = cardset.get("code", "")
        name = cardset.get("name", "")
        # Non-set buckets that bigweb uses for damaged/related/misc items
        skip_codes = {"傷あり", "関連商品", "EX・スターター", "デッキ販売"}
        skip_keywords = ["傷あり", "関連商品", "特製デッキ"]
        if code in skip_codes:
            return True
        return any(kw in name for kw in skip_keywords)

    # ------------------------------------------------------------------
    # Card parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _is_single_card(item: dict) -> bool:
        """Return True if item is a tradeable single card (not a box/accessory)."""
        if item.get("is_box"):
            return False
        rarity_web = (item.get("rarity") or {}).get("web", "")
        if not rarity_web or rarity_web == "-":
            return False
        if not item.get("comment", "").strip():
            return False
        return True

    @staticmethod
    def _extract_card_number(comment: str) -> str:
        """Extract card number from comment, stripping trailing store notes."""
        m = _CARD_NUMBER_RE.match(comment.strip())
        return m.group() if m else comment.strip()

    def _item_to_listing(self, item: dict, set_code: str) -> ShopListing:
        comment = item.get("comment", "").strip()
        card_number_raw = self._extract_card_number(comment)
        rarity = item.get("rarity") or {}
        card_cond = item.get("card_condition")

        return ShopListing(
            shop=self.shop,
            tcg=self.tcg,
            set_code=set_code,
            card_number_raw=card_number_raw,
            card_name_raw=item.get("name", ""),
            rarity_raw=rarity.get("web", ""),
            condition="NM",
            price=float(item["price"]),
            currency="JPY",
            quantity=item.get("stock_count", 0),
            url=f"{BIGWEB_WEB_BASE}/ja/products/{self.game_code}/{item['id']}",
            crawled_at=datetime.now(timezone.utc),
            extra={
                "product_id": item["id"],
                "bigweb_cardset_id": (item.get("cardset") or {}).get("id"),
                "card_condition_web": (card_cond or {}).get("web") if card_cond else None,
                "comment_raw": comment,
                "is_sold_out": item.get("is_sold_out", False),
            },
        )

    # ------------------------------------------------------------------
    # ShopCrawler interface
    # ------------------------------------------------------------------

    def crawl_set(self, set_code: str) -> Iterator[ShopListing]:
        """Crawl all single-card listings for a set by its code (e.g. 'B01').

        Looks up the cardset ID from the fetched set list.
        """
        sets = self.fetch_sets()
        matched = [s for s in sets if s.get("code", "").upper() == set_code.upper()]
        if not matched:
            logger.warning("Set code %s not found in bigweb cardsets", set_code)
            return
        cardset = matched[0]
        yield from self._crawl_cardset(cardset)

    def _crawl_cardset(self, cardset: dict) -> Iterator[ShopListing]:
        """Yield all single-card listings for a bigweb cardset dict."""
        cardset_id = cardset["id"]
        set_code = cardset.get("code", "")
        set_name = cardset.get("name", "")
        page = 1
        total_pages = 1

        logger.info("Crawling %s (%s) cardset_id=%s", set_code, set_name[:30], cardset_id)

        while page <= total_pages:
            data = self._get_json(
                f"{BIGWEB_API_BASE}/products",
                params={"game_id": self.game_id, "cardsets": cardset_id, "page": page},
            )
            pag = data.get("pagenate", {})
            total_pages = pag.get("pageCount", 1)

            for item in data.get("items", []):
                if self._is_single_card(item):
                    yield self._item_to_listing(item, set_code)

            page += 1

    def search_card(self, card_number: str) -> Iterator[ShopListing]:
        raise NotImplementedError(
            "Bigweb is crawled per-set. Use crawl_set() with the appropriate set code."
        )

    # ------------------------------------------------------------------
    # Full crawl orchestration
    # ------------------------------------------------------------------

    def run_full_crawl(self, db_path=None) -> None:
        """Crawl all sets and persist listings to DuckDB.

        Skips sets whose listings were already crawled today.
        """
        conn = get_connection(db_path or DB_PATH)
        init_schema(conn)
        _init_bigweb_schema(conn)

        cardsets = self.fetch_sets()
        if not cardsets:
            logger.error("No cardsets discovered — check game_id=%s", self.game_id)
            return

        # Skip sets already crawled today
        today = datetime.now(timezone.utc).date().isoformat()
        crawled_today: set[str] = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT set_code FROM raw_shop_listings "
                "WHERE shop = ? AND tcg = ? AND crawled_at::DATE = ?",
                [self.shop, self.tcg, today],
            ).fetchall()
        }

        to_crawl = [s for s in cardsets if s.get("code") not in crawled_today]
        logger.info(
            "%d/%d sets already crawled today, crawling %d remaining",
            len(crawled_today), len(cardsets), len(to_crawl),
        )

        for cardset in to_crawl:
            set_code = cardset.get("code", "")
            batch: list[dict] = []
            count = 0

            try:
                for listing in self._crawl_cardset(cardset):
                    batch.append({
                        "shop": listing.shop,
                        "tcg": listing.tcg,
                        "set_code": listing.set_code,
                        "card_number_raw": listing.card_number_raw,
                        "card_name_raw": listing.card_name_raw,
                        "rarity_raw": listing.rarity_raw,
                        "condition": listing.condition,
                        "price": listing.price,
                        "currency": listing.currency,
                        "quantity": listing.quantity,
                        "url": listing.url,
                        "crawled_at": listing.crawled_at,
                        "extra": json.dumps(listing.extra, ensure_ascii=False),
                    })
                    count += 1
                    if len(batch) >= 200:
                        insert_shop_listings(conn, batch)
                        batch.clear()
            except Exception:
                logger.exception("Failed to crawl set %s — skipping", set_code)
                continue

            if batch:
                insert_shop_listings(conn, batch)

            conn.execute(
                """INSERT OR REPLACE INTO bigweb_cardsets
                       (game_id, cardset_id, set_code, set_name, last_crawled_at, listing_count)
                   VALUES (?, ?, ?, ?, now(), ?)""",
                [self.game_id, cardset["id"], set_code, cardset.get("name", ""), count],
            )
            logger.info("   saved %d listings for %s", count, set_code)

        conn.close()
        logger.info("Bigweb full crawl complete (game_id=%s)", self.game_id)
