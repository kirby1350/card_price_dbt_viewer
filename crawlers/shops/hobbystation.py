"""Hobby Station (hobbystation-single.jp) shop crawler — Union Arena singles.

Site overview
-------------
Hobby Station is a major Japanese TCG retailer.  The Union Arena section lists
~10,000 singles across booster packs, starter decks, and promos.
The site is EC-CUBE based; product data is server-rendered HTML.

URL patterns
------------
  UA hub page    : https://www.hobbystation-single.jp/ua/top
  Set listing    : https://www.hobbystation-single.jp/ua/product/list
                     ?HbstSearchOptions[0][id]=62
                     &HbstSearchOptions[0][search_keyword]=(BANNER)...【UA46BT】(BANNER)
                     &HbstSearchOptions[0][Type]=2
                     &pageno=1&disp_number=60&orderby=5
  Product detail : https://www.hobbystation-single.jp/ua/product/detail/{product_id}

Listing HTML structure
----------------------
Each card on a listing page is an <li> element whose ``id`` attribute is the
Hobby Station card number:

  <li id="UA-UA46BT-KGR-1-060">
    <a href="/ua/product/detail/436405">
      <img src="...">
    </a>
    <a href="...">六平 千鉱</a>
    <p>1,580円 在庫数: ◎</p>
  </li>

Parallel variants append a suffix: ``UA-UA46BT-KGR-1-060-01`` (★★),
``-02`` (★★★).  The product name carries (★), (★★), or (★★★).

Card number conversion
----------------------
  Hobby Station : UA-UA46BT-KGR-1-060(-01)
  Standard      : UA46BT/KGR-1-060

  1. Strip ``UA-`` prefix
  2. Replace first ``-`` after set code with ``/``
  3. Strip trailing variant suffix ``-NN`` for parallels

Pagination
----------
  60 items per page (disp_number=60).
  Presence of <a> with text "次へ" means more pages exist.

What is stored (raw_shop_listings, shop="hobbystation", tcg="unionarena")
-------------------------------------------------------------------------
  card_number_raw  e.g. "UA46BT/KGR-1-060"
  card_name_raw    e.g. "六平 千鉱"
  rarity_raw       e.g. "", "★", "★★", "★★★" (parallel star level)
  price            tax-included JPY (float)
  currency         "JPY"
  condition        "NM" — hobbystation does not grade by condition
  quantity         0 = SOLD OUT, -1 = ◎ (abundant), else numeric
  set_code         extracted from card_number_raw, e.g. "UA46BT"
  url              individual product page URL
  extra.product_id integer product ID from URL
  extra.hs_card_id Hobby Station card ID (li id attribute)
"""

import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Iterator
from urllib.parse import urlparse, parse_qs, urljoin, unquote

import requests
from bs4 import BeautifulSoup

from crawlers.shops.base import ShopCrawler, ShopListing
from crawlers.storage import DB_PATH, get_connection, init_schema, insert_shop_listings

logger = logging.getLogger(__name__)

BASE = "https://www.hobbystation-single.jp"
UA_TOP = f"{BASE}/ua/top"
LIST_URL = f"{BASE}/ua/product/list"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,zh-CN;q=0.9",
}

# Set code patterns in the Hobby Station card ID (after stripping UA- prefix).
# Matches: UA46BT, UA48ST, EX13BT, PC02BT, UAPR, UCPR, etc.
_SET_CODE_RE = re.compile(
    r"^((?:UA\d+(?:BT|ST|NC)|EX\d+BT|PC\d+BT|UAPR|UCPR)\w*)"
)

# Parallel star indicator in product name: (★), (★★), (★★★) or full-width
_STARS_RE = re.compile(r"[（(](★+)[）)]$")

# Variant suffix at end of Hobby Station card ID: -01, -02, etc.
_VARIANT_SUFFIX_RE = re.compile(r"-(\d{2})$")

# Price: extract digits from "1,580円"
_PRICE_RE = re.compile(r"[\d,]+")

# Stock: "在庫数: 3" or "在庫数: ◎"
_STOCK_RE = re.compile(r"在庫数[:：]\s*(\S+)")

# Product ID from URL: /ua/product/detail/436405
_PRODUCT_ID_RE = re.compile(r"/product/detail/(\d+)")

# Set code in brackets from sidebar link text: 【UA46BT】
_SET_CODE_BRACKET_RE = re.compile(r"【([A-Za-z0-9]+)】")

# HbstSearchOptions in sidebar href
_SEARCH_OPTS_RE = re.compile(r"HbstSearchOptions")


def _convert_card_number(hs_id: str, has_stars: bool) -> tuple[str | None, str | None]:
    """Convert a Hobby Station card ID to standard card number.

    Args:
        hs_id: e.g. "UA-UA46BT-KGR-1-060" or "UA-UA46BT-KGR-1-060-01"
        has_stars: True if the product name has ★ (parallel variant)

    Returns:
        (card_number, set_code) or (None, None) if not parseable.
        card_number in standard format, e.g. "UA46BT/KGR-1-060"
    """
    # Strip UA- prefix
    if not hs_id.startswith("UA-"):
        return None, None
    rest = hs_id[3:]  # e.g. "UA46BT-KGR-1-060-01"

    # Strip variant suffix for parallels
    if has_stars:
        rest = _VARIANT_SUFFIX_RE.sub("", rest)

    # Find set code
    m = _SET_CODE_RE.match(rest)
    if not m:
        return None, None
    set_code = m.group(1)  # e.g. "UA46BT"

    # Rest after set code should start with "-"
    remainder = rest[len(set_code):]
    if not remainder.startswith("-"):
        return None, None

    card_part = remainder[1:]  # e.g. "KGR-1-060"
    card_number = f"{set_code}/{card_part}"
    return card_number, set_code


def _parse_price(text: str) -> float | None:
    m = _PRICE_RE.search(text)
    return float(m.group().replace(",", "")) if m else None


def _parse_stock(text: str) -> int:
    """Parse stock from listing text.

    Returns: -1 for ◎ (abundant), 0 for SOLD OUT, else numeric count.
    """
    if "SOLD" in text or "sold" in text:
        return 0
    m = _STOCK_RE.search(text)
    if m:
        val = m.group(1)
        if val == "◎":
            return -1
        try:
            return int(val)
        except ValueError:
            return 0
    return 0


class HobbystationShopCrawler(ShopCrawler):
    """Shop price crawler for hobbystation-single.jp (Union Arena).

    Args:
        delay: seconds to sleep between HTTP requests (default 1.0)
    """

    shop = "hobbystation"
    tcg = "unionarena"

    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._sets: list[dict] = []  # [{set_code, label, url}, ...]

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
    # Set discovery from /ua/top sidebar
    # ------------------------------------------------------------------

    def fetch_sets(self) -> list[dict]:
        """Scrape /ua/top for set listing URLs.

        Sidebar links are image-based (no text). Set codes are extracted
        from the URL-encoded ``search_keyword`` parameter which contains
        the set name with 【SET_CODE】 brackets.

        Returns list of dicts: {set_code, label, url}.
        """
        if self._sets:
            return self._sets

        soup = self._get_html(UA_TOP)
        seen: set[str] = set()
        sets: list[dict] = []

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if "HbstSearchOptions" not in href:
                continue

            # Build absolute URL
            url = href if href.startswith("http") else urljoin(BASE, href)

            # Extract set code from the search_keyword parameter in the URL
            parsed = urlparse(url)
            qs = parse_qs(parsed.query, keep_blank_values=True)
            keyword_raw = qs.get("HbstSearchOptions[0][search_keyword]", [""])[0]
            keyword = unquote(keyword_raw)
            # keyword looks like: "(BANNER)カグラバチ【UA46BT】(BANNER)"

            sc_m = _SET_CODE_BRACKET_RE.search(keyword)
            if not sc_m:
                # Links without set code (e.g. promo category) — skip
                continue
            set_code = sc_m.group(1)

            # Build label from keyword (strip BANNER markers)
            label = keyword.replace("(BANNER)", "").strip()

            # Deduplicate by set_code
            if set_code in seen:
                continue
            seen.add(set_code)

            sets.append({
                "set_code": set_code,
                "label": label,
                "url": url,
            })

        self._sets = sets
        logger.info("UA top: found %d sets", len(sets))
        return sets

    # ------------------------------------------------------------------
    # Listing page parsing
    # ------------------------------------------------------------------

    def _parse_listing_page(self, soup: BeautifulSoup) -> list[ShopListing]:
        """Extract all ShopListings from one parsed listing page.

        Each product is an <li> containing:
          <div style="...lightcyan...">UA-UA46BT-KGR-1-060</div>  (card ID)
          <figure><a href="/ua/product/detail/NNN">...</a></figure>
          <div class="list_product_Name_pc"><a>六平 千鉱（★）</a></div>
          <div class="packageDetail">1,580円 <span class="stock">在庫数: ◎</span></div>
        """
        listings: list[ShopListing] = []
        now = datetime.now(timezone.utc)

        for li in soup.find_all("li"):
            # Card ID is in a <div> with lightcyan background
            card_div = li.find("div", style=lambda s: s and "lightcyan" in s)
            if not card_div:
                continue
            hs_id = card_div.get_text(strip=True)
            if not hs_id.startswith("UA-"):
                continue

            # Product name from list_product_Name_pc (or _sp)
            name_div = li.find("div", class_="list_product_Name_pc")
            if not name_div:
                name_div = li.find("div", class_="list_product_Name_sp")
            name_text = name_div.get_text(strip=True) if name_div else ""
            if not name_text:
                continue

            # Product URL and ID
            product_url = ""
            product_id = None
            for a_tag in li.find_all("a", href=True):
                pid_m = _PRODUCT_ID_RE.search(a_tag["href"])
                if pid_m:
                    product_id = int(pid_m.group(1))
                    href = a_tag["href"]
                    product_url = href if href.startswith("http") else f"{BASE}{href}"
                    break
            if not product_url:
                continue

            # Parse star level from name: (★), (★★), (★★★)
            stars_m = _STARS_RE.search(name_text)
            star_count = len(stars_m.group(1)) if stars_m else 0
            has_stars = star_count > 0

            # Clean card name (remove star suffix)
            card_name = _STARS_RE.sub("", name_text).strip()

            # Convert card number
            card_number, set_code = _convert_card_number(hs_id, has_stars)
            if not card_number:
                logger.debug("Cannot parse card number from id=%r, skipping", hs_id)
                continue

            # Rarity raw: star indicators for parallels
            rarity_raw = "★" * star_count

            # Price from packageDetail div
            pkg_div = li.find("div", class_="packageDetail")
            if not pkg_div:
                continue
            pkg_text = pkg_div.get_text(" ", strip=True)
            price = _parse_price(pkg_text)
            if price is None:
                continue

            # Stock
            quantity = _parse_stock(pkg_text)

            listings.append(ShopListing(
                shop=self.shop,
                tcg=self.tcg,
                set_code=set_code,
                card_number_raw=card_number,
                card_name_raw=card_name,
                rarity_raw=rarity_raw,
                condition="NM",
                price=price,
                currency="JPY",
                quantity=quantity,
                url=product_url,
                crawled_at=now,
                extra={
                    "product_id": product_id,
                    "hs_card_id": hs_id,
                },
            ))

        return listings

    # ------------------------------------------------------------------
    # Paginated set crawl
    # ------------------------------------------------------------------

    def _iter_set_url(self, set_url: str) -> Iterator[ShopListing]:
        """Yield all listings from a set URL, paginating automatically."""
        # Parse original URL and add pagination params
        parsed = urlparse(set_url)
        base_params = parse_qs(parsed.query, keep_blank_values=True)

        page = 1
        while True:
            # Build params for this page
            params = {}
            for k, v in base_params.items():
                params[k] = v[0] if len(v) == 1 else v
            params["pageno"] = str(page)
            params["disp_number"] = "60"
            params["orderby"] = "5"  # card number ascending

            soup = self._get_html(LIST_URL, params=params)
            listings = self._parse_listing_page(soup)
            yield from listings

            # Stop if no items or no "次へ" (next page) link
            if not listings:
                break
            next_link = soup.find("a", string=re.compile(r"次へ"))
            if not next_link:
                break
            page += 1

    # ------------------------------------------------------------------
    # ShopCrawler interface
    # ------------------------------------------------------------------

    def crawl_set(self, set_code: str) -> Iterator[ShopListing]:
        """Yield all listings for a given set code.

        Finds the set URL from the sidebar, then paginates through it.
        """
        set_code_upper = set_code.upper()
        for s in self.fetch_sets():
            if s["set_code"].upper() == set_code_upper:
                yield from self._iter_set_url(s["url"])
                return
        logger.warning("Set code %r not found in sidebar", set_code)

    def search_card(self, card_number: str) -> Iterator[ShopListing]:
        """Not implemented — Hobby Station search is keyword-based."""
        raise NotImplementedError("Hobby Station search requires keyword, not card number")

    # ------------------------------------------------------------------
    # Full crawl orchestration
    # ------------------------------------------------------------------

    def run_full_crawl(self, db_path=None, conn=None) -> None:
        """Crawl all Hobby Station UA sets and persist listings to DB.

        Skips sets already crawled today.

        Args:
            db_path: DuckDB file path (default: data/raw.duckdb).
            conn:    Pre-opened connection (DuckDB or PgAdapter). When provided,
                     db_path is ignored and the caller is responsible for closing.
        """
        _own_conn = conn is None
        if _own_conn:
            conn = get_connection(db_path or DB_PATH)
        init_schema(conn)

        sets = self.fetch_sets()
        if not sets:
            logger.warning("No sets found on UA top page")
            return

        # Skip sets already crawled today
        today = datetime.now(timezone.utc).date().isoformat()
        crawled_today: set[str] = set()
        try:
            rows = conn.execute(
                "SELECT DISTINCT set_code FROM raw_shop_listings "
                "WHERE shop = ? AND tcg = ? AND crawled_at::DATE = ?",
                [self.shop, self.tcg, today],
            ).fetchall()
            for (sc,) in rows:
                if sc:
                    crawled_today.add(sc.upper())
        except Exception:
            pass

        to_crawl = [s for s in sets if s["set_code"].upper() not in crawled_today]
        logger.info(
            "%d/%d sets already crawled today, crawling %d remaining",
            len(crawled_today), len(sets), len(to_crawl),
        )

        total_saved = 0
        for s in to_crawl:
            logger.info("→ %s (%s)", s["set_code"], s["label"])
            batch: list[dict] = []
            count = 0

            try:
                for listing in self._iter_set_url(s["url"]):
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
                logger.exception("Failed to crawl set %s — skipping", s["set_code"])
                continue

            if batch:
                insert_shop_listings(conn, batch)

            total_saved += count
            logger.info("  saved %d listings", count)

        if _own_conn:
            conn.close()
        logger.info("Hobby Station full crawl complete — %d listings saved", total_saved)
