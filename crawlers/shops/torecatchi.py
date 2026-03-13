"""Torecatchi (torecatchi.com) shop crawler — Union Arena singles only.

Site overview
-------------
Torecatchi is a Union Arena-specialist shop. Every product is a UA single.
The site is server-rendered HTML; there is no JSON API.

URL patterns
------------
  Category (paged)      : https://www.torecatchi.com/product-list/{cat_id}?page={N}
  Individual product    : https://www.torecatchi.com/product/{product_id}
  Sitemap               : https://www.torecatchi.com/sitemap.xml

Listing HTML structure (actual, verified from live pages)
----------------------------------------------------------
Each card on a listing page appears as:

  <div class="item_data">
    <a class="item_data_link" href="/product/{id}">
      <div class="inner_item_data">
        <div class="list_item_data">
          <p class="item_name">
            <span class="goods_name">{full_title}</span>
          </p>
          <div class="item_info">
            <div class="price">
              <p class="selling_price">
                <span class="figure">{price}<span class="currency_label after_price">円</span></span>
              </p>
            </div>
          </div>
        </div>
      </div>
    </a>
  </div>

The goods_name text format is:
  "{set_code}/{series}-{seq} {rarity}\u3000{name}"
e.g.  "UA02BT/JJK-1-005 SR\u3000狗巻 棘"
      "UA08BT/BLC-1-004 SR★\u3000エス・ノト【パラレル】"
      "EX13BT/IMA-1-001 R\u3000如月 千都"

Pagination
----------
  60 items per page (fixed, count= param has no effect).
  Presence of <a class="to_next_page"> means more pages exist.

Individual product page
-----------------------
  - H1 contains same full title as goods_name
  - "在庫数{N}点" in page text = N copies available
  - "在庫なし" in page text = out of stock

What is stored (raw_shop_listings, shop="torecatchi", tcg="unionarena")
-----------------------------------------------------------------------
  card_number_raw  e.g. "UA01BT/CGH-1-001"
  card_name_raw    e.g. "扇 要"
  rarity_raw       e.g. "U", "SR", "SR★", "SR★★", "SR★★★"
  price            tax-included JPY (float)
  currency         "JPY"
  condition        "NM" — torecatchi does not grade by condition
  quantity         0 (unknown) unless fetch_quantity=True
  set_code         extracted from card_number_raw, e.g. "UA01BT"
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
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup

from crawlers.shops.base import ShopCrawler, ShopListing
from crawlers.storage import DB_PATH, get_connection, init_schema, insert_shop_listings

logger = logging.getLogger(__name__)

BASE = "https://www.torecatchi.com"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,zh-CN;q=0.9",
}

# goods_name: "UA01BT/CGH-1-001 U\u3000扇 要"
# Group 1: card number  (e.g. UA01BT/CGH-1-001)
# Group 2: rarity       (e.g. U, SR, SR★, SR★★★)
# Group 3: card name    (after ideographic space U+3000)
_GOODS_NAME_RE = re.compile(
    r"^((?:UA|EX|PC)\d+BT/[A-Z]+-\d+-\d+)\s+([A-Z★+]+)\u3000(.+)$"
)

# Newer category pages (e.g. EX13BT) omit the card number from the listing.
# Two sub-formats, both require fetching the product detail page for the card number:
#   【パラレル】葛城 リーリヤ\u3000SR★★   (parallel cards)
#   葛城 リーリヤ\u3000SR                 (regular cards, no prefix)
# The card number appears in the product detail page H1:
#   "葛城 リーリヤ　SR[EX13BT/GIM-2-001]"
# Items WITHOUT \u3000{rarity} (e.g. 【AP】, 【シリアルAP】, 【青】) are skipped.
_NEW_FORMAT_RE = re.compile(
    r"^(?:【[^】]*】)?(.+)\u3000([A-Z★]+)$"
)
# Card number in brackets at end of detail-page H1
_CARD_NO_IN_H1_RE = re.compile(
    r"\[((?:UA|EX|PC)\d+BT/[A-Z]+-\d+-\d+(?:_p\d+)?)\]"
)

# set_code from card_number: "UA01BT" from "UA01BT/CGH-1-001"
_SET_CODE_RE = re.compile(r"^((?:UA|EX|PC)\d+BT)")

# Price: extract leading digits from "1,080円" or "100円"
_PRICE_RE = re.compile(r"[\d,]+")

# Quantity on product page: "在庫数8点" (N in stock)
_STOCK_RE = re.compile(r"在庫数(\d+)点")


def _parse_goods_name(text: str) -> tuple[str, str, str] | None:
    """Parse a goods_name string into (card_number, rarity, card_name).

    Returns None for non-standard items (promos, AP cards, deck accessories),
    and for new-format parallel listings where the card number is absent
    (those are handled separately via the detail page).
    """
    m = _GOODS_NAME_RE.match(text.strip())
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3)


def _parse_price(text: str) -> float | None:
    m = _PRICE_RE.search(text)
    return float(m.group().replace(",", "")) if m else None


class TorecatchiShopCrawler(ShopCrawler):
    """Shop price crawler for torecatchi.com (Union Arena specialist).

    Args:
        delay:          seconds to sleep between HTTP requests (default 0.8)
        fetch_quantity: if True, follow each product URL to read stock count.
                        Accurate but adds one request per card; disabled by default.
    """

    shop = "torecatchi"
    tcg = "unionarena"

    def __init__(self, delay: float = 0.8, fetch_quantity: bool = False):
        self.delay = delay
        self.fetch_quantity = fetch_quantity
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

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

    def _get_raw(self, url: str) -> str:
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        time.sleep(self.delay)
        return resp.text

    # ------------------------------------------------------------------
    # Category discovery via sitemap
    # ------------------------------------------------------------------

    def fetch_category_ids(self) -> list[int]:
        """Parse sitemap.xml and return all /product-list/{id} category IDs."""
        raw = self._get_raw(f"{BASE}/sitemap.xml")
        try:
            root = ElementTree.fromstring(raw)
        except ElementTree.ParseError:
            logger.warning("Failed to parse sitemap XML")
            return []

        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        pattern = re.compile(r"/product-list/(\d+)$")
        ids: list[int] = []
        seen: set[int] = set()

        for loc in root.findall(".//sm:loc", ns):
            if loc.text is None:
                continue
            m = pattern.search(loc.text)
            if m:
                cat_id = int(m.group(1))
                if cat_id not in seen:
                    seen.add(cat_id)
                    ids.append(cat_id)

        ids.sort()
        logger.info("Sitemap: found %d product-list category IDs", len(ids))
        return ids

    # ------------------------------------------------------------------
    # Listing page parsing
    # ------------------------------------------------------------------

    def _parse_listing_page(
        self, soup: BeautifulSoup, category_id: int | None
    ) -> list[ShopListing]:
        """Extract all ShopListings from one parsed listing page."""
        listings: list[ShopListing] = []
        now = datetime.now(timezone.utc)

        for item in soup.find_all("div", class_="item_data"):
            # --- goods_name: card number + rarity + name ---
            name_span = item.find("span", class_="goods_name")
            if not name_span:
                continue
            goods_text = name_span.get_text(strip=True)
            parsed = _parse_goods_name(goods_text)

            if not parsed:
                # Try the newer name\u3000rarity format (e.g. EX13BT regular/parallel cards).
                # Card number is absent from the listing page — must fetch detail.
                pm = _NEW_FORMAT_RE.match(goods_text)
                if not pm:
                    logger.debug("Unrecognised goods_name, skipping: %r", goods_text)
                    continue
                card_name_raw = pm.group(1).strip()
                rarity_raw = pm.group(2)

                # Need product URL to fetch card number
                link = item.find("a", class_="item_data_link")
                if not link or not link.get("href"):
                    continue
                href = link["href"]
                product_url = href if href.startswith("http") else f"{BASE}{href}"
                pid_m = re.search(r"/product/(\d+)", href)
                product_id = int(pid_m.group(1)) if pid_m else None

                card_number_raw, quantity = self._fetch_detail(product_url)
                if not card_number_raw:
                    logger.debug("Could not get card number from detail page %s", product_url)
                    continue

                set_code_m = _SET_CODE_RE.match(card_number_raw)
                set_code = set_code_m.group(1) if set_code_m else None

                figure = item.find("span", class_="figure")
                price = _parse_price(figure.get_text(strip=True)) if figure else None
                if price is None:
                    continue

                listings.append(ShopListing(
                    shop=self.shop, tcg=self.tcg, set_code=set_code,
                    card_number_raw=card_number_raw, card_name_raw=card_name_raw,
                    rarity_raw=rarity_raw, condition="NM",
                    price=price, currency="JPY", quantity=quantity,
                    url=product_url, crawled_at=now,
                    extra={"product_id": product_id, "category_id": category_id},
                ))
                continue

            card_number_raw, rarity_raw, card_name_raw = parsed

            # set_code from card_number prefix
            m = _SET_CODE_RE.match(card_number_raw)
            set_code = m.group(1) if m else None

            # --- price: span.figure contains "100" + nested "円" ---
            figure = item.find("span", class_="figure")
            price = _parse_price(figure.get_text(strip=True)) if figure else None
            if price is None:
                continue

            # --- product URL and ID ---
            link = item.find("a", class_="item_data_link")
            if link and link.get("href"):
                href = link["href"]
                product_url = href if href.startswith("http") else f"{BASE}{href}"
                pid_m = re.search(r"/product/(\d+)", href)
                product_id = int(pid_m.group(1)) if pid_m else None
            else:
                product_url = BASE
                product_id = None

            listings.append(ShopListing(
                shop=self.shop,
                tcg=self.tcg,
                set_code=set_code,
                card_number_raw=card_number_raw,
                card_name_raw=card_name_raw,
                rarity_raw=rarity_raw,
                condition="NM",
                price=price,
                currency="JPY",
                quantity=0,         # populated below if fetch_quantity=True
                url=product_url,
                crawled_at=now,
                extra={
                    "product_id": product_id,
                    "category_id": category_id,
                },
            ))

        return listings

    def _fetch_detail(self, url: str) -> tuple[str | None, int]:
        """Fetch a product detail page; return (card_number_or_None, quantity).

        Used for new-format listings that omit the card number on the listing page.
        """
        try:
            soup = self._get_html(url)
            h1 = soup.find("h1")
            card_number = None
            if h1:
                m = _CARD_NO_IN_H1_RE.search(h1.get_text())
                if m:
                    card_number = m.group(1)
            text = soup.get_text()
            qty_m = _STOCK_RE.search(text)
            quantity = int(qty_m.group(1)) if qty_m else 0
            return card_number, quantity
        except Exception:
            logger.debug("Failed to fetch detail from %s", url)
            return None, 0

    def _fetch_quantity(self, url: str) -> int:
        """Fetch a product page and return stock quantity (0 = out of stock)."""
        _, qty = self._fetch_detail(url)
        return qty

    # ------------------------------------------------------------------
    # Crawl a single category (all pages)
    # ------------------------------------------------------------------

    def _iter_category(self, category_id: int | None) -> Iterator[ShopListing]:
        """Yield all listings from a category, paginating automatically.

        The site returns 60 items per page regardless of the count= parameter.
        Pagination ends when there is no <a class="to_next_page"> link.
        """
        base_url = (
            f"{BASE}/product-list/{category_id}" if category_id is not None
            else f"{BASE}/product-list"
        )
        page = 1
        while True:
            soup = self._get_html(base_url, params={"page": page})
            listings = self._parse_listing_page(soup, category_id)

            if self.fetch_quantity:
                for lst in listings:
                    lst.quantity = self._fetch_quantity(lst.url)

            yield from listings

            # Stop if no "next page" link or no items returned
            if not listings or not soup.find("a", class_="to_next_page"):
                break
            page += 1

    # ------------------------------------------------------------------
    # ShopCrawler interface
    # ------------------------------------------------------------------

    def crawl_set(self, set_code: str) -> Iterator[ShopListing]:
        """Yield all listings whose set_code matches the given code.

        Because torecatchi organises by category (not set code), this crawls
        all products and filters by set_code prefix. Prefer run_full_crawl
        for a full scrape.
        """
        set_code_upper = set_code.upper()
        for listing in self._iter_category(None):
            if listing.set_code == set_code_upper:
                yield listing

    def search_card(self, card_number: str) -> Iterator[ShopListing]:
        """Yield listings matching card_number exactly (scans all products)."""
        for listing in self._iter_category(None):
            if listing.card_number_raw == card_number:
                yield listing

    # ------------------------------------------------------------------
    # Full crawl orchestration
    # ------------------------------------------------------------------

    def run_full_crawl(self, db_path=None, conn=None) -> None:
        """Crawl all torecatchi categories and persist listings to DB.

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
            logger.warning("No category IDs from sitemap — crawling all products")
            category_ids = [None]

        # Skip categories already crawled today by checking which category_ids
        # already have rows in raw_shop_listings from today.
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
        logger.info("Torecatchi full crawl complete — %d listings saved", total_saved)
