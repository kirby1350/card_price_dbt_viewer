"""Masters Square (masters-square.com) shop crawler — Union Arena singles.

Site overview
-------------
Masters Square is a Japanese TCG retailer with a dedicated Union Arena section.
The site is server-rendered HTML; there is no JSON API.

URL patterns
------------
  UA hub page          : https://www.masters-square.com/page/33
  Category (paged)     : https://www.masters-square.com/product-list/{cat_id}?page={N}
  Individual product   : https://www.masters-square.com/product/{product_id}

Listing format
--------------
Each card on a listing page is an <a> linking to the product page.
The product title (both in alt text and a text node) has this format:

  Regular   : "漣 京羅[UA_UA46BT/KGR-1-001_SR]"
  Parallel  : "【星3パラレル】花海 咲季[UA_EX13BT/GIM-2-029_SR]"
  AP card   : "アクションポイントカード(花海 咲季)[UA_EX13BT/GIM-2-AP01_AP]"
  Parallel+no rarity : "【パラレル】アクションポイントカード(六平 千鉱)[UA_UA46BT/KGR-1-AP01]"

The "[UA_...]" portion encodes:
  UA_{set_code}/{series}-{seq}_{rarity}
  e.g. UA_UA46BT/KGR-1-001_SR → card_number=UA46BT/KGR-1-001, rarity=SR

Pagination
----------
  Next page link text: "次»"

What is stored (raw_shop_listings, shop="mastersquare", tcg="unionarena")
-------------------------------------------------------------------------
  card_number_raw  e.g. "UA46BT/KGR-1-001"
  card_name_raw    e.g. "漣 京羅"
  rarity_raw       e.g. "SR", "SR★", "SR★★", "SR★★★", "R", "U", "C", "AP"
  price            tax-included JPY (float)
  currency         "JPY"
  condition        "NM" — mastersquare does not grade by condition
  quantity         stock count from listing page (0 = out of stock / unknown)
  set_code         extracted from card_number_raw, e.g. "UA46BT"
  url              individual product page URL
  extra.product_id integer product ID from URL
  extra.category_id the category ID this listing came from
"""

import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Iterator

import requests
from bs4 import BeautifulSoup

from crawlers.shops.base import ShopCrawler, ShopListing
from crawlers.storage import DB_PATH, get_connection, init_schema, insert_shop_listings

logger = logging.getLogger(__name__)

BASE = "https://www.masters-square.com"
UA_HUB = f"{BASE}/page/33"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,zh-CN;q=0.9",
}

# Title pattern: "カード名[UA_SET/SERIES-N-NNN_RARITY]"
# Group 1: everything before the bracket (card name, possibly with 【...】 prefix)
# Group 2: card code inside brackets, e.g. "UA_UA46BT/KGR-1-001_SR"
_TITLE_RE = re.compile(r"^(.+?)\[UA_((?:UA|EX|PC)\d+\w*/[A-Z]+-\d+-[A-Za-z0-9]+(?:_p\d+)?(?:_[A-Z★+]+)?)\]$")

# Split the code portion: "UA46BT/KGR-1-001_SR" → card_number + rarity
# Card number: everything up to the last underscore (if rarity follows)
# Some codes have no rarity suffix: "UA46BT/KGR-1-AP01"
_CODE_RARITY_RE = re.compile(r"^(.+?)(?:_([A-Z★+]+))?$")

# set_code from card_number: "UA46BT" from "UA46BT/KGR-1-001"
_SET_CODE_RE = re.compile(r"^((?:UA|EX|PC)\d+\w*)")

# Parallel prefix: 【パラレル】 or 【星Nパラレル】
_PARALLEL_RE = re.compile(r"^【(星(\d+))?パラレル】(.+)$")

# Price: extract digits from "180円(税込)" or "1,080円(税込)"
_PRICE_RE = re.compile(r"[\d,]+")

# Stock: "在庫数 16点"
_STOCK_RE = re.compile(r"在庫数\s*(\d+)\s*点")

# Product ID from URL: /product/12345
_PRODUCT_ID_RE = re.compile(r"/product/(\d+)")

# Category ID from URL: /product-list/1013
_CATEGORY_RE = re.compile(r"/product-list/(\d+)")

# Skip patterns: complete sets, decks, sleeves, boxes, etc.
_SKIP_PATTERNS = re.compile(
    r"コンプ[リセ]|構築済み|デッキ|スリーブ|ボックス|BOX|box|パック|"
    r"\[UA_deck\]|\[UA_.*?/C\d+\]",
    re.IGNORECASE,
)

# Link text pattern that indicates a Union Arena category
# Matches set codes like "UA46BT", "EX13BT", "PC02BT" or UA keywords
_UA_LINK_RE = re.compile(
    r"(?:UA|EX|PC)\d+|ユニオンアリーナ|UNION\s*ARENA|プロモーション",
    re.IGNORECASE,
)


def _parse_title(text: str) -> dict | None:
    """Parse a product title into card components.

    Returns a dict with keys: card_number, card_name, rarity, is_parallel, stars
    or None if the title doesn't match the card format.
    """
    text = text.strip()

    # Skip non-card items
    if _SKIP_PATTERNS.search(text):
        return None

    m = _TITLE_RE.match(text)
    if not m:
        return None

    raw_name = m.group(1).strip()
    code_part = m.group(2)  # e.g. "UA46BT/KGR-1-001_SR"

    # Split code into card_number and rarity
    cm = _CODE_RARITY_RE.match(code_part)
    if not cm:
        return None
    card_number = cm.group(1)  # e.g. "UA46BT/KGR-1-001"
    rarity = cm.group(2)      # e.g. "SR" or None

    # Check for parallel prefix
    is_parallel = False
    stars = 0
    pm = _PARALLEL_RE.match(raw_name)
    if pm:
        is_parallel = True
        stars = int(pm.group(2)) if pm.group(2) else 1
        raw_name = pm.group(3).strip()

    # Build rarity_raw: for parallels, encode star level
    if rarity and is_parallel and stars > 0:
        rarity_raw = rarity + "★" * stars
    elif rarity:
        rarity_raw = rarity
    else:
        rarity_raw = ""

    return {
        "card_number": card_number,
        "card_name": raw_name,
        "rarity": rarity_raw,
        "is_parallel": is_parallel,
        "stars": stars,
    }


def _parse_price(text: str) -> float | None:
    m = _PRICE_RE.search(text)
    return float(m.group().replace(",", "")) if m else None


class MastersSquareShopCrawler(ShopCrawler):
    """Shop price crawler for masters-square.com (Union Arena).

    Args:
        delay: seconds to sleep between HTTP requests (default 1.0)
    """

    shop = "mastersquare"
    tcg = "unionarena"

    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._category_ids: list[int] = []

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get_html(self, url: str, params: dict | None = None) -> BeautifulSoup:
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                resp.encoding = "utf-8"
                time.sleep(self.delay)
                return BeautifulSoup(resp.text, "lxml")
            except Exception as exc:
                if attempt == 2:
                    raise
                wait = 5 * (attempt + 1)
                logger.warning("Request failed (%s), retrying in %ds…", exc, wait)
                time.sleep(wait)
                self.session = requests.Session()
                self.session.headers.update(HEADERS)
        raise RuntimeError("unreachable")

    # ------------------------------------------------------------------
    # Category discovery from the UA hub page (/page/33)
    # ------------------------------------------------------------------

    def fetch_category_ids(self) -> list[int]:
        """Scrape /page/33 for Union Arena /product-list/{id} links.

        Filters links by text content: only includes links whose label
        contains a UA set code pattern (UA##, EX##, PC##) or UA keywords.
        This excludes navigation links to other TCGs.
        """
        if self._category_ids:
            return self._category_ids

        soup = self._get_html(UA_HUB)
        seen: set[int] = set()
        ids: list[int] = []

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            m = _CATEGORY_RE.search(href)
            if not m:
                continue
            # Filter: only UA-related links (by link text or surrounding text)
            link_text = a_tag.get_text(strip=True)
            if not _UA_LINK_RE.search(link_text):
                continue
            cat_id = int(m.group(1))
            if cat_id not in seen:
                seen.add(cat_id)
                ids.append(cat_id)

        ids.sort()
        self._category_ids = ids
        logger.info("UA hub: found %d product-list category IDs", len(ids))
        return ids

    # ------------------------------------------------------------------
    # Listing page parsing
    # ------------------------------------------------------------------

    def _parse_listing_page(
        self, soup: BeautifulSoup, category_id: int | None,
    ) -> list[ShopListing]:
        """Extract all ShopListings from one parsed listing page."""
        listings: list[ShopListing] = []
        now = datetime.now(timezone.utc)

        # Find all product links — each card is an <a> to /product/{id}
        for a_tag in soup.find_all("a", href=_PRODUCT_ID_RE):
            href = a_tag.get("href", "")
            pid_m = _PRODUCT_ID_RE.search(href)
            if not pid_m:
                continue
            product_id = int(pid_m.group(1))
            product_url = href if href.startswith("http") else f"{BASE}{href}"

            # Get the product title — try img alt first, then text content
            title = None
            img = a_tag.find("img")
            if img and img.get("alt"):
                title = img["alt"].strip()
            if not title:
                title = a_tag.get_text(strip=True)
            if not title:
                continue

            # Parse the title
            parsed = _parse_title(title)
            if not parsed:
                logger.debug("Skipping non-card item: %r", title)
                continue
            if not parsed["rarity"]:
                logger.debug("No rarity, skipping: %r", title)
                continue

            card_number = parsed["card_number"]
            set_code_m = _SET_CODE_RE.match(card_number)
            set_code = set_code_m.group(1) if set_code_m else None

            # Find price and stock in sibling/child text
            block_text = a_tag.get_text("\n", strip=True)

            # Price
            price = None
            for line in block_text.split("\n"):
                if "円" in line:
                    price = _parse_price(line)
                    if price is not None:
                        break
            if price is None:
                continue

            # Stock
            quantity = 0
            stock_m = _STOCK_RE.search(block_text)
            if stock_m:
                quantity = int(stock_m.group(1))

            listings.append(ShopListing(
                shop=self.shop,
                tcg=self.tcg,
                set_code=set_code,
                card_number_raw=card_number,
                card_name_raw=parsed["card_name"],
                rarity_raw=parsed["rarity"],
                condition="NM",
                price=price,
                currency="JPY",
                quantity=quantity,
                url=product_url,
                crawled_at=now,
                extra={
                    "product_id": product_id,
                    "category_id": category_id,
                },
            ))

        return listings

    # ------------------------------------------------------------------
    # Crawl a single category (all pages)
    # ------------------------------------------------------------------

    def _iter_category(self, category_id: int) -> Iterator[ShopListing]:
        """Yield all listings from a category, paginating automatically."""
        base_url = f"{BASE}/product-list/{category_id}"
        page = 1
        while True:
            soup = self._get_html(base_url, params={"page": page})
            listings = self._parse_listing_page(soup, category_id)
            yield from listings

            # Stop if no items or no "次»" (next page) link
            if not listings:
                break
            next_link = soup.find("a", string=re.compile(r"次"))
            if not next_link:
                break
            page += 1

    # ------------------------------------------------------------------
    # ShopCrawler interface
    # ------------------------------------------------------------------

    def crawl_set(self, set_code: str) -> Iterator[ShopListing]:
        """Yield all listings whose set_code matches the given code.

        Because Masters Square organises by category (not set code), this
        crawls all categories and filters by set_code prefix.
        """
        set_code_upper = set_code.upper()
        for cat_id in self.fetch_category_ids():
            for listing in self._iter_category(cat_id):
                if listing.set_code == set_code_upper:
                    yield listing

    def search_card(self, card_number: str) -> Iterator[ShopListing]:
        """Not implemented — Masters Square has no search API."""
        raise NotImplementedError("Masters Square does not expose a search API")

    # ------------------------------------------------------------------
    # Full crawl orchestration
    # ------------------------------------------------------------------

    def run_full_crawl(self, db_path=None, conn=None) -> None:
        """Crawl all Masters Square UA categories and persist listings to DB.

        Skips categories already crawled today.

        Args:
            db_path: DuckDB file path (default: data/raw.duckdb).
            conn:    Pre-opened connection (DuckDB or PgAdapter). When provided,
                     db_path is ignored and the caller is responsible for closing.
        """
        _own_conn = conn is None
        if _own_conn:
            conn = get_connection(db_path or DB_PATH)
        init_schema(conn)

        category_ids = self.fetch_category_ids()
        if not category_ids:
            logger.warning("No category IDs found on UA hub page")
            return

        # Skip categories already crawled today
        today = datetime.now(timezone.utc).date().isoformat()
        crawled_today: set[str] = set()
        try:
            rows = conn.execute(
                "SELECT DISTINCT extra FROM raw_shop_listings "
                "WHERE shop = ? AND tcg = ? AND crawled_at::DATE = ?",
                [self.shop, self.tcg, today],
            ).fetchall()
            for (extra_str,) in rows:
                if not extra_str:
                    continue
                try:
                    cat = json.loads(extra_str).get("category_id")
                    if cat is not None:
                        crawled_today.add(str(cat))
                except Exception:
                    pass
        except Exception:
            pass

        to_crawl = [c for c in category_ids if str(c) not in crawled_today]
        logger.info(
            "%d/%d categories already crawled today, crawling %d remaining",
            len(crawled_today), len(category_ids), len(to_crawl),
        )

        total_saved = 0
        for cat_id in to_crawl:
            logger.info("→ category %s", cat_id)
            batch: list[dict] = []
            count = 0

            try:
                for listing in self._iter_category(cat_id):
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
                logger.exception("Failed to crawl category %s — skipping", cat_id)
                continue

            if batch:
                insert_shop_listings(conn, batch)

            total_saved += count
            logger.info("  saved %d listings", count)

        if _own_conn:
            conn.close()
        logger.info("Masters Square full crawl complete — %d listings saved", total_saved)
