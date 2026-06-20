"""Fukufuku Toreka (fukufukutoreka.com) shop crawler — multi-TCG.

Site overview
-------------
福福トレカ is a Japanese TCG retailer running one colorme (Shop-Pro / EC-CUBE
style) storefront per game on a dedicated subdomain.  This crawler is generic:
instantiate it with a ``tcg`` and it targets that game's subdomain.

  unionarena → https://uniari.fukufukutoreka.com
  yugioh     → https://yugioh.fukufukutoreka.com
  weiss      → https://weis.fukufukutoreka.com

URL patterns (identical across subdomains)
------------------------------------------
  Category (paged) : {base}/products/list?category_id={cat_id}&pageno={N}
  Product detail   : {base}/products/detail/{product_id}

Listing HTML structure (verified from live pages)
-------------------------------------------------
Each card is an ``<li class="product-list__item">``:

  <li class="product-list__item">
    <h2 class="product-list__item__title">
      <a class="product-list__item__title--name" href=".../products/detail/11678">
        {full_title}
      </a>
    </h2>
    <table class="product-list__item__table">
      <td class="product-list__item__table--price">￥1,580</td>
      <td class="product-list__item__table--count">
        <select class="list-qty-select"><option value="1">1</option>...</select>
        <span>/N</span>
      </td>
    </table>
  </li>

The ``{full_title}`` format differs per game:

  Union Arena : "【状態B】日暮 かごめ(SR★)(UA50BT/IYS-1-066)"
                  name(rarity)(card_number)   — last two paren groups
  Weiss       : "灰の魔女(AGR)(GA17/S131-033A)"   — same as UA;
                base cards may omit the rarity group: "リュー(SBY/W114-003)"
  Yu-Gi-Oh    : "ウィッチクラフト【OSE】〈RV01-JP038〉"
                  name【rarity】〈card_number〉  — rarity in 【】, number in 〈〉

Card names may themselves contain parentheses (e.g. UA action-point cards), so
the card number is identified by pattern, not merely "the last group".

Pagination
----------
  48 items per page.  The maximum ``pageno`` is read from the pagination links;
  crawling also stops early when a page yields no products.

Stock / quantity
----------------
  The purchasable quantity is the largest value offered by ``.list-qty-select``.
  Out-of-stock items have no select → quantity 0.

What is stored (raw_shop_listings, shop="fukufuku")
---------------------------------------------------
  card_number_raw  e.g. "UA50BT/IYS-1-066", "GA17/S131-033A", "RV01-JP038"
  card_name_raw    card name with the rarity / number / condition markers removed
  rarity_raw       e.g. "SR", "SR★", "AGR", "OSE" ("" when the shop omits it)
  price            tax-included JPY (float)
  currency         "JPY"
  condition        "NM", or the 状態 grade (A/B/C) when the title marks one
  quantity         purchasable copies (0 = out of stock)
  set_code         derived from card_number (e.g. "UA50BT", "GA17/S131", "RV01")
  url              individual product page URL
  extra.product_id    integer product ID from the detail URL
  extra.category_id   the category ID this listing came from
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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,zh-CN;q=0.9",
}

# Per-TCG site configuration: subdomain base URL + title-parsing strategy.
_CONFIG = {
    "unionarena": {"base": "https://uniari.fukufukutoreka.com", "parser": "ua"},
    "yugioh":     {"base": "https://yugioh.fukufukutoreka.com", "parser": "ygo"},
    "weiss":      {"base": "https://weis.fukufukutoreka.com",   "parser": "ws"},
}

# Card-number patterns, used to locate the number among parenthesised groups.
_UA_CARDNO_RE = re.compile(r"^(?:UA|EX|PC)\d+BT/[A-Z]+-\d+-[A-Za-z0-9]+$")
_WS_CARDNO_RE = re.compile(r"^[A-Za-z0-9]+/[A-Z]+\d+-\d+[A-Za-z0-9]*$")

# A plausible rarity token: optional "Pc" prefix, letters, and/or stars.
_RARITY_RE = re.compile(r"^(?:Pc)?[A-Za-z]{0,8}[★]*$")

# Half-width and full-width parenthesised groups (non-nested).
_PAREN_RE = re.compile(r"[(（]([^()（）]*)[)）]")

# Yu-Gi-Oh: rarity in 【】, card number in 〈〉 (allow full/half-width angles).
_YGO_RARITY_RE = re.compile(r"【([^】]+)】")
_YGO_CARDNO_RE = re.compile(r"[〈<]([^〉>]+)[〉>]")

# Leading condition markers, e.g. 【状態B】.  Group 1 = grade letter when present.
_CONDITION_RE = re.compile(r"状態\s*([A-DＡ-Ｄ])")

# Trailing ★ on a card code (AP parallels): "UA51BT/SLG-1-AP02★" → strip.
_TRAILING_STARS_RE = re.compile(r"[★]+$")

# Product ID from a detail URL: /products/detail/11678
_PRODUCT_ID_RE = re.compile(r"/products/detail/(\d+)")

# category_id in a list URL.
_CATEGORY_RE = re.compile(r"category_id=(\d+)")

# pageno in a pagination link.
_PAGENO_RE = re.compile(r"pageno=(\d+)")

# Price digits from "￥1,580" / "1,580円".
_PRICE_RE = re.compile(r"[\d,]+")


def _extract_condition(title: str) -> str:
    """Return a 状態 grade letter found in the title, else 'NM'."""
    m = _CONDITION_RE.search(title)
    if m:
        # Normalise full-width grade letters to ASCII.
        grade = m.group(1)
        return grade.translate(str.maketrans("ＡＢＣＤ", "ABCD"))
    return "NM"


def _ua_ws_set_code(card_number: str) -> str | None:
    """set_code from a UA/Weiss card number.

    UA   : "UA50BT/IYS-1-066" → "UA50BT"
    Weiss: "GA17/S131-033A"   → "GA17/S131" (everything before the last '-')
    """
    if "/" not in card_number:
        return None
    head = card_number.split("/", 1)[0]
    if re.match(r"^(?:UA|EX|PC)\d+BT$", head):
        return head
    idx = card_number.rfind("-")
    return card_number[:idx] if idx != -1 else card_number


def _parse_paren_title(title: str, cardno_re: re.Pattern) -> dict | None:
    """Parse a "name(rarity)(card_number)" title (Union Arena / Weiss).

    The card number is the parenthesised group matching ``cardno_re``; the rarity
    is the immediately preceding group when it looks like a rarity token.
    Returns None if no card number is found.
    """
    groups = list(_PAREN_RE.finditer(title))
    if not groups:
        return None

    # Find the card-number group (search right-to-left).
    cardno_idx = None
    card_number = None
    for i in range(len(groups) - 1, -1, -1):
        candidate = groups[i].group(1).strip()
        stripped = _TRAILING_STARS_RE.sub("", candidate)
        if cardno_re.match(stripped):
            cardno_idx = i
            card_number = stripped
            break
    if card_number is None:
        return None

    # Rarity = preceding group if it looks like a rarity token.
    rarity = ""
    name_end = groups[cardno_idx].start()
    if cardno_idx > 0:
        prev = groups[cardno_idx - 1].group(1).strip()
        if prev and "/" not in prev and _RARITY_RE.match(prev):
            rarity = prev[2:] if prev.startswith("Pc") else prev
            name_end = groups[cardno_idx - 1].start()

    card_name = title[:name_end]
    # Strip a leading condition marker like 【状態B】 from the name.
    card_name = re.sub(r"^【[^】]*】", "", card_name).strip()

    return {"card_number": card_number, "rarity": rarity, "card_name": card_name}


def _parse_ygo_title(title: str) -> dict | None:
    """Parse a "name【rarity】〈card_number〉" Yu-Gi-Oh title."""
    nm = _YGO_CARDNO_RE.search(title)
    if not nm:
        return None
    card_number = nm.group(1).strip()

    # Rarity = the last 【】 group occurring before the 〈〉 number.
    rarity = ""
    name_end = nm.start()
    rar_matches = [m for m in _YGO_RARITY_RE.finditer(title) if m.end() <= nm.start()]
    if rar_matches:
        last = rar_matches[-1]
        rarity = last.group(1).strip()
        name_end = last.start()

    card_name = title[:name_end].strip()
    return {"card_number": card_number, "rarity": rarity, "card_name": card_name}


def _ygo_set_code(card_number: str) -> str | None:
    """'RV01-JP038' → 'RV01', '20CP-JPC02' → '20CP'."""
    return card_number.split("-", 1)[0] if "-" in card_number else card_number


def _parse_price(text: str) -> float | None:
    m = _PRICE_RE.search(text)
    return float(m.group().replace(",", "")) if m else None


class FukufukuShopCrawler(ShopCrawler):
    """Shop price crawler for fukufukutoreka.com (multi-TCG, per-subdomain).

    Args:
        tcg:   one of "unionarena", "yugioh", "weiss".
        delay: seconds to sleep between HTTP requests (default 1.0).
    """

    shop = "fukufuku"

    def __init__(self, tcg: str, delay: float = 1.0):
        if tcg not in _CONFIG:
            raise ValueError(f"Unsupported tcg {tcg!r}; expected one of {list(_CONFIG)}")
        self.tcg = tcg
        self.base = _CONFIG[tcg]["base"]
        self._parser = _CONFIG[tcg]["parser"]
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
    # Category discovery from the storefront top page
    # ------------------------------------------------------------------

    def fetch_category_ids(self) -> list[int]:
        """Scrape the top page for every /products/list?category_id=N link."""
        if self._category_ids:
            return self._category_ids

        soup = self._get_html(f"{self.base}/")
        seen: set[int] = set()
        ids: list[int] = []
        for a_tag in soup.find_all("a", href=True):
            m = _CATEGORY_RE.search(a_tag["href"])
            if not m:
                continue
            cat_id = int(m.group(1))
            if cat_id not in seen:
                seen.add(cat_id)
                ids.append(cat_id)

        ids.sort()
        self._category_ids = ids
        logger.info("%s top page: found %d category IDs", self.tcg, len(ids))
        return ids

    # ------------------------------------------------------------------
    # Title parsing
    # ------------------------------------------------------------------

    def _parse_title(self, title: str) -> dict | None:
        title = title.strip()
        if self._parser == "ygo":
            parsed = _parse_ygo_title(title)
            set_fn = _ygo_set_code
        elif self._parser == "ws":
            parsed = _parse_paren_title(title, _WS_CARDNO_RE)
            set_fn = _ua_ws_set_code
        else:  # "ua"
            parsed = _parse_paren_title(title, _UA_CARDNO_RE)
            set_fn = _ua_ws_set_code
        if not parsed:
            return None
        parsed["set_code"] = set_fn(parsed["card_number"])
        parsed["condition"] = _extract_condition(title)
        return parsed

    # ------------------------------------------------------------------
    # Listing page parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_quantity(li) -> int:
        """Purchasable quantity = largest value offered by the qty select."""
        sel = li.select_one(".list-qty-select")
        if not sel:
            return 0
        vals = [
            int(o["value"])
            for o in sel.find_all("option")
            if o.get("value", "").isdigit()
        ]
        return max(vals) if vals else 0

    def _parse_listing_page(
        self, soup: BeautifulSoup, category_id: int | None,
    ) -> list[ShopListing]:
        listings: list[ShopListing] = []
        now = datetime.now(timezone.utc)

        for li in soup.select("li.product-list__item"):
            link = li.select_one("h2.product-list__item__title a") or li.select_one(
                'a[href*="/products/detail/"]'
            )
            if not link:
                continue
            title = link.get_text(strip=True)
            parsed = self._parse_title(title)
            if not parsed:
                logger.debug("Unparseable title, skipping: %r", title)
                continue

            href = link.get("href", "")
            pid_m = _PRODUCT_ID_RE.search(href)
            product_id = int(pid_m.group(1)) if pid_m else None
            product_url = href if href.startswith("http") else f"{self.base}{href}"

            price_td = li.select_one("td.product-list__item__table--price")
            price = _parse_price(price_td.get_text(strip=True)) if price_td else None
            if price is None:
                continue

            listings.append(ShopListing(
                shop=self.shop,
                tcg=self.tcg,
                set_code=parsed["set_code"],
                card_number_raw=parsed["card_number"],
                card_name_raw=parsed["card_name"],
                rarity_raw=parsed["rarity"],
                condition=parsed["condition"],
                price=price,
                currency="JPY",
                quantity=self._parse_quantity(li),
                url=product_url,
                crawled_at=now,
                extra={"product_id": product_id, "category_id": category_id},
            ))

        return listings

    # ------------------------------------------------------------------
    # Crawl a single category (all pages)
    # ------------------------------------------------------------------

    def _iter_category(self, category_id: int) -> Iterator[ShopListing]:
        """Yield all listings from a category, paginating automatically."""
        list_url = f"{self.base}/products/list"
        page = 1
        max_page = 1
        while True:
            soup = self._get_html(
                list_url, params={"category_id": category_id, "pageno": page}
            )
            listings = self._parse_listing_page(soup, category_id)
            yield from listings

            if page == 1:
                # Determine the last page from the pagination links.
                pagenos = {
                    int(m.group(1))
                    for a in soup.find_all("a", href=True)
                    for m in [_PAGENO_RE.search(a["href"])]
                    if m
                }
                max_page = max(pagenos) if pagenos else 1

            if not listings or page >= max_page:
                break
            page += 1

    # ------------------------------------------------------------------
    # ShopCrawler interface
    # ------------------------------------------------------------------

    def crawl_set(self, set_code: str) -> Iterator[ShopListing]:
        """Yield listings whose set_code matches, by scanning all categories.

        Fukufuku organises by category, not set code, so this filters the full
        crawl. Prefer run_full_crawl for a complete scrape.
        """
        set_code_upper = set_code.upper()
        for cat_id in self.fetch_category_ids():
            for listing in self._iter_category(cat_id):
                if listing.set_code and listing.set_code.upper() == set_code_upper:
                    yield listing

    def search_card(self, card_number: str) -> Iterator[ShopListing]:
        """Not implemented — fukufuku has no per-card search endpoint here."""
        raise NotImplementedError("Fukufuku is crawled by category, not card number")

    # ------------------------------------------------------------------
    # Full crawl orchestration
    # ------------------------------------------------------------------

    def run_full_crawl(self, db_path=None, conn=None) -> None:
        """Crawl all categories for this TCG and persist listings to the DB.

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
            logger.warning("No category IDs found on %s top page", self.base)
            return

        # Skip categories already crawled today.
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
        logger.info("Fukufuku (%s) full crawl complete — %d listings saved",
                    self.tcg, total_saved)
