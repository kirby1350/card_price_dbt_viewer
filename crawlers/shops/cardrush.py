"""Card Rush shop crawler — covers cardrush.jp / cardrush-vanguard.jp / cardrush-digimon.jp.

All three sites run on the same platform with identical URL structures.

URL structure
-------------
  Group/set index : {base_url}/group
  Set page        : {base_url}/product-group/{id}?display=100&page={n}
  Product page    : {base_url}/product/{id}

HTML structure (set page)
--------------------------
  <div class="ajax_itemlist_box ...">
    <ul>
      <li>
        <a href="/product/{id}">
          <img ... />
          〔状態X〕(optional notes)CardName【Rarity】{CardNumber}《Type》
          X,XXX円(税込)
          在庫数 X枚
        </a>
      </li>
      ...
    </ul>
  </div>
  <div class="...pager...">
    <strong>1</strong>
    <a href="?display=100&page=2">2</a>
    ...
  </div>

Title parse order
-----------------
  1. Optional condition  : 〔状態A〕 / 〔状態A-〕 / 〔状態B〕 / 〔状態C〕 / 〔状態D〕
  2. Optional notes      : (05)(パラレル/illus:Name) ...
  3. Card name           : Japanese text up to 【Rarity】
  4. Rarity              : 【RarityName】
  5. Card number         : {CODE-XXX}
  6. Optional card type  : 《Type》

Condition mapping
-----------------
  A, A+ → "NM"
  A-    → "NM-"
  B     → "LP"
  C     → "MP"
  D     → "HP"
  (none) → "NM"
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

ITEMS_PER_PAGE = 100

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,zh-CN;q=0.9",
}

_CONDITION_MAP = {
    "A":  "NM",
    "A+": "NM",
    "A-": "NM-",
    "B":  "LP",
    "C":  "MP",
    "D":  "HP",
}

_CONDITION_RE   = re.compile(r"〔状態([A-D][+\-]?)〕")
_RARITY_RE      = re.compile(r"【([^】]+)】")
_CARD_NUMBER_RE = re.compile(r"\{([^}]+)\}")
_PRICE_RE       = re.compile(r"([\d,]+)円")
_STOCK_RE       = re.compile(r"在庫数\s*(\d+)枚")
_LEADING_NOTES  = re.compile(r"^(\([^)]*\)\s*)+")   # (05)(パラレル/illus:Name) …
_PRODUCT_GROUP_RE = re.compile(r"/product-group/(\d+)")
# Asia-version marker in YGO titles  e.g. "〔アジア版〕" or "(アジア)" or card number ending in -AE/-AS/-AP
_ASIA_RE = re.compile(r"アジア")


def _init_cardrush_schema(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cardrush_product_groups (
            shop             VARCHAR  NOT NULL,
            product_group_id INTEGER  NOT NULL,
            group_name       VARCHAR,
            last_crawled_at  TIMESTAMPTZ,
            listing_count    INTEGER,
            PRIMARY KEY (shop, product_group_id)
        );
    """)


# ---------------------------------------------------------------------------
# Title parsing helpers
# ---------------------------------------------------------------------------

def _parse_title(raw: str) -> dict:
    """Decompose a Card Rush product title into its constituent fields.

    Input example:
      "〔状態A-〕(05)(パラレル/illus:Name)クローズ【R-P】{EX11-065}《黒》"

    Returns a dict with keys:
      condition, card_name_raw, rarity_raw, card_number_raw, notes
    """
    text = raw.strip()

    # 1. Condition
    m = _CONDITION_RE.match(text)
    if m:
        condition = _CONDITION_MAP.get(m.group(1), "NM")
        text = text[m.end():]
    else:
        condition = "NM"

    # 2. Leading parenthetical notes
    m = _LEADING_NOTES.match(text)
    notes = m.group(0).strip() if m else ""
    if m:
        text = text[m.end():]

    # 3. Rarity 【...】
    rarity_raw = ""
    m = _RARITY_RE.search(text)
    if m:
        rarity_raw = m.group(1)
        card_name = text[: m.start()].strip()
        remainder = text[m.end():]
    else:
        card_name = text.strip()
        remainder = ""

    # 4. Card number {CODE-XXX}
    card_number_raw = ""
    m = _CARD_NUMBER_RE.search(remainder)
    if m:
        card_number_raw = m.group(1)

    # 5. Asia-version flag (YGO only, but checked generically)
    is_asia = bool(_ASIA_RE.search(raw))

    return {
        "condition":       condition,
        "card_name_raw":   card_name,
        "rarity_raw":      rarity_raw,
        "card_number_raw": card_number_raw,
        "notes":           notes,
        "is_asia":         is_asia,
    }


def _set_code_from_card_number(card_number: str) -> str | None:
    """Extract the set code prefix from a card number by splitting on the last separator.

    "EX11-030"   → "EX11"
    "LOCH-JP012" → "LOCH"   (last '-' separates set from card number)
    "D-BT24/001" → "D-BT24" (last '/' separates set from card number)
    "SUB1-JP040" → "SUB1"
    """
    parts = re.split(r"[-/](?=[^-/]*$)", card_number)
    if len(parts) >= 2:
        return parts[0].upper()
    return None


# ---------------------------------------------------------------------------
# Crawler class
# ---------------------------------------------------------------------------

class CardRushShopCrawler(ShopCrawler):
    """Shop price crawler for the Card Rush family of sites.

    Instantiate once per site:

      CardRushShopCrawler(
          "https://www.cardrush.jp",          "cardrush",          "yugioh",
          index_url="https://www.cardrush.jp/page/35")
      CardRushShopCrawler(
          "https://www.cardrush-vanguard.jp", "cardrush-vanguard", "vanguard",
          index_url="https://www.cardrush-vanguard.jp/category")
      CardRushShopCrawler(
          "https://www.cardrush-digimon.jp",  "cardrush-digimon",  "digimon",
          index_url="https://www.cardrush-digimon.jp/category")

    Args:
        base_url:  Root URL of the shop (no trailing slash).
        shop:      Canonical shop identifier stored in raw_shop_listings.shop.
        tcg:       Canonical TCG identifier (e.g. "yugioh", "vanguard", "digimon").
        delay:     Seconds to sleep between HTTP requests.
        index_url: Page that lists all product-group links for this site.
                   Defaults to {base_url}/category.
    """

    def __init__(
        self,
        base_url: str,
        shop: str,
        tcg: str,
        delay: float = 1.0,
        index_url: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.shop = shop
        self.tcg = tcg
        self.delay = delay
        self.index_url = index_url or f"{self.base_url}/category"
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._product_groups: list[dict] = []

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _get(self, url: str, **params) -> BeautifulSoup:
        resp = self.session.get(url, params=params or None, timeout=30)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        time.sleep(self.delay)
        return BeautifulSoup(resp.text, "lxml")

    # ------------------------------------------------------------------
    # Set / product-group discovery
    # ------------------------------------------------------------------

    def fetch_sets(self) -> list[dict]:
        """Discover all product groups from the index page."""
        if self._product_groups:
            return self._product_groups
        logger.info("Fetching product group list from %s", self.index_url)
        soup = self._get(self.index_url)
        self._product_groups = self._parse_product_groups(soup)
        logger.info("Found %d product groups", len(self._product_groups))
        return self._product_groups

    def _parse_product_groups(self, soup: BeautifulSoup) -> list[dict]:
        """Extract product-group links from the /group index page."""
        groups: list[dict] = []
        seen: set[int] = set()
        for a in soup.find_all("a", href=_PRODUCT_GROUP_RE):
            href = a.get("href", "")
            m = _PRODUCT_GROUP_RE.search(href)
            if not m:
                continue
            pg_id = int(m.group(1))
            if pg_id in seen:
                continue
            seen.add(pg_id)
            # Prefer visible text; fall back to img alt
            name = a.get_text(strip=True)
            if not name:
                img = a.find("img", alt=True)
                name = img["alt"] if img else ""
            groups.append({
                "product_group_id": pg_id,
                "group_name":       name,
                "url":              f"{self.base_url}/product-group/{pg_id}",
            })
        return groups

    # ------------------------------------------------------------------
    # Listing parsing
    # ------------------------------------------------------------------

    def _parse_page(
        self, soup: BeautifulSoup, product_group_id: int
    ) -> tuple[list[ShopListing], int]:
        """Parse one listing page. Returns (listings, total_pages).

        HTML structure (confirmed from live pages):
          div.itemlist_box
            ul
              li.list_item_cell.list_item_{id}
                div.item_data[data-product-id]
                  a.item_data_link[href]
                    p.item_name > span.goods_name   ← full title text
                    div.item_info
                      div.price > p.selling_price > span.figure   ← "260円"
                      p.stock                                      ← "在庫数 5枚"
        """
        listings: list[ShopListing] = []

        container = soup.find("div", class_="itemlist_box")
        if not container:
            return listings, 1

        for li in container.find_all("li", class_=re.compile(r"list_item_cell")):
            item_div = li.find("div", class_="item_data")
            if not item_div:
                continue

            # Product URL
            a = item_div.find("a", class_="item_data_link")
            if not a:
                continue
            href = a.get("href", "")
            product_url = href if href.startswith("http") else f"{self.base_url}{href}"

            # Product ID
            product_id = item_div.get("data-product-id")

            # Title
            name_span = a.find("span", class_="goods_name")
            if not name_span:
                continue
            title_line = name_span.get_text(strip=True)
            if not title_line:
                continue

            # Price
            figure_span = a.find("span", class_="figure")
            if not figure_span:
                continue
            m = _PRICE_RE.search(figure_span.get_text(strip=True))
            if not m:
                continue
            price = float(m.group(1).replace(",", ""))

            # Stock
            stock_p = a.find("p", class_="stock")
            quantity = 0
            if stock_p:
                m2 = _STOCK_RE.search(stock_p.get_text(strip=True))
                if m2:
                    quantity = int(m2.group(1))

            parsed = _parse_title(title_line)
            set_code = (
                _set_code_from_card_number(parsed["card_number_raw"]) or str(product_group_id)
                if parsed["card_number_raw"]
                else str(product_group_id)
            )

            listings.append(ShopListing(
                shop=self.shop,
                tcg=self.tcg,
                set_code=set_code,
                card_number_raw=parsed["card_number_raw"],
                card_name_raw=parsed["card_name_raw"],
                rarity_raw=parsed["rarity_raw"],
                condition=parsed["condition"],
                price=price,
                currency="JPY",
                quantity=quantity,
                url=product_url,
                crawled_at=datetime.now(timezone.utc),
                extra={
                    "product_id":       product_id,
                    "product_group_id": product_group_id,
                    "notes":            parsed["notes"],
                    "is_asia":          parsed["is_asia"],
                },
            ))

        # Total pages: highest page number in pager_btn links
        total_pages = 1
        for tag in soup.find_all("a", class_="pager_btn"):
            m = re.search(r"[?&]page=(\d+)", tag.get("href", ""))
            if m:
                total_pages = max(total_pages, int(m.group(1)))

        return listings, total_pages

    def _crawl_product_group(self, product_group_id: int) -> Iterator[ShopListing]:
        """Crawl all pages of a single product group."""
        url = f"{self.base_url}/product-group/{product_group_id}"
        logger.info("Crawling product-group %d — %s", product_group_id, url)

        page = 1
        total_pages = 1
        while page <= total_pages:
            soup = self._get(url, display=ITEMS_PER_PAGE, page=page)
            batch, total_pages = self._parse_page(soup, product_group_id)
            logger.debug("  page %d/%d → %d listings", page, total_pages, len(batch))
            yield from batch
            page += 1

    # ------------------------------------------------------------------
    # ShopCrawler interface
    # ------------------------------------------------------------------

    def crawl_set(self, set_code: str) -> Iterator[ShopListing]:
        """Crawl a single product group by its numeric ID.

        Args:
            set_code: String representation of the product-group ID (e.g. "189").
        """
        try:
            pg_id = int(set_code)
        except ValueError:
            logger.error(
                "CardRush crawl_set expects a numeric product-group ID, got %r", set_code
            )
            return
        yield from self._crawl_product_group(pg_id)

    def search_card(self, card_number: str) -> Iterator[ShopListing]:
        raise NotImplementedError(
            "Card Rush is crawled per product-group. Use crawl_set() with a product_group_id."
        )

    # ------------------------------------------------------------------
    # Full crawl orchestration
    # ------------------------------------------------------------------

    def run_full_crawl(self, db_path=None) -> None:
        """Crawl all product groups and persist listings to DuckDB.

        Skips groups that were already crawled today (tracked via
        cardrush_product_groups.last_crawled_at).
        """
        conn = get_connection(db_path or DB_PATH)
        init_schema(conn)
        _init_cardrush_schema(conn)

        groups = self.fetch_sets()
        if not groups:
            logger.error("No product groups found at %s/group", self.base_url)
            return

        today = datetime.now(timezone.utc).date().isoformat()
        already_crawled: set[int] = {
            r[0]
            for r in conn.execute(
                "SELECT product_group_id FROM cardrush_product_groups "
                "WHERE shop = ? AND last_crawled_at::DATE = ?",
                [self.shop, today],
            ).fetchall()
        }

        to_crawl = [g for g in groups if g["product_group_id"] not in already_crawled]
        logger.info(
            "%d/%d groups already crawled today, crawling %d remaining",
            len(already_crawled), len(groups), len(to_crawl),
        )

        for group in to_crawl:
            pg_id = group["product_group_id"]
            batch: list[dict] = []
            count = 0

            try:
                for listing in self._crawl_product_group(pg_id):
                    batch.append({
                        "shop":            listing.shop,
                        "tcg":             listing.tcg,
                        "set_code":        listing.set_code,
                        "card_number_raw": listing.card_number_raw,
                        "card_name_raw":   listing.card_name_raw,
                        "rarity_raw":      listing.rarity_raw,
                        "condition":       listing.condition,
                        "price":           listing.price,
                        "currency":        listing.currency,
                        "quantity":        listing.quantity,
                        "url":             listing.url,
                        "crawled_at":      listing.crawled_at,
                        "extra":           json.dumps(listing.extra, ensure_ascii=False),
                    })
                    count += 1
                    if len(batch) >= 200:
                        insert_shop_listings(conn, batch)
                        batch.clear()
            except Exception:
                logger.exception("Failed to crawl product-group %d — skipping", pg_id)
                continue

            if batch:
                insert_shop_listings(conn, batch)

            conn.execute(
                """INSERT OR REPLACE INTO cardrush_product_groups
                       (shop, product_group_id, group_name, last_crawled_at, listing_count)
                   VALUES (?, ?, ?, now(), ?)""",
                [self.shop, pg_id, group.get("group_name", ""), count],
            )
            logger.info(
                "   saved %d listings for group %d (%s)",
                count, pg_id, group.get("group_name", ""),
            )

        conn.close()
        logger.info("Card Rush full crawl complete (%s)", self.base_url)
