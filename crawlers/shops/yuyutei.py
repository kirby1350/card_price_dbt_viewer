"""YuYuTei (yuyu-tei.jp) shop crawler — generic across all TCGs they carry.

URL structure
-------------
  Index/top : https://yuyu-tei.jp/top/{game_code}
  Set page  : https://yuyu-tei.jp/sell/{game_code}/s/{set_code_lower}
  Card page : https://yuyu-tei.jp/sell/{game_code}/card/{set_code_lower}/{product_id}

Known game codes
----------------
  zx   → Z/X -Zillions of enemy X-
  ygo  → Yu-Gi-Oh!
  (add more as needed)

HTML structure (set page, confirmed from live page)
----------------------------------------------------
Set list in sidebar:
  <button id="side-sell-zx-s-b01" onclick="location.href='...s/b01'">
    [B01] 異世界との邂逅
  </button>

Rarity sections (each is a <div class="py-4 cards-list" id="card-list3">):
  <h3>
    <span class="py-2 d-inline-block px-2 me-2 text-white fw-bold">SRH</span>
    Card List
  </h3>
  <div class="row mt-2" id="card-lits">
    <div class="col-md">
      <div class="card-product position-relative mt-4 [sold-out]">
        <a href="/sell/zx/card/b01/10201"> <img alt="B01-101 Z/XR 滅界勇者 織田信長"> </a>
        <span class="d-block border border-dark p-1 w-100 text-center my-2">B01-101</span>
        <a href="/sell/zx/card/b01/10201"><h4 class="text-primary fw-bold">Card Name</h4></a>
        <strong class="d-block text-end">220 円</strong>
        <label class="cart_sell_zaiko">在庫 : 5 点</label>
        <input class="cart_limit" type="hidden" value="5"/>   ← 0 = sold out
        <input class="cart_cid"   type="hidden" value="10201"/>
      </div>
      ...
    </div>
  </div>

Stock: cart_limit=0 → sold out; cart_limit=N → N copies available.
All yuyu-tei singles are Near Mint (NM) unless noted in card_name_raw.
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Iterator

import requests
from bs4 import BeautifulSoup, Tag

from crawlers.shops.base import ShopCrawler, ShopListing
from crawlers.storage import DB_PATH, get_connection, init_schema, insert_shop_listings

logger = logging.getLogger(__name__)

YUYUTEI_BASE = "https://yuyu-tei.jp"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,zh-CN;q=0.9",
}

_PRICE_RE = re.compile(r"[\d,]+")


def _init_yuyutei_schema(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS yuyutei_sets (
            game_code       VARCHAR NOT NULL,
            set_code        VARCHAR NOT NULL,
            set_name        VARCHAR,
            last_crawled_at TIMESTAMPTZ,
            listing_count   INTEGER,
            PRIMARY KEY (game_code, set_code)
        );
    """)


class YuyuteiShopCrawler(ShopCrawler):
    """Shop price crawler for yuyu-tei.jp.

    Generic: instantiate with any game_code to crawl that TCG's listings.

    Args:
        game_code: yuyu-tei's internal code, e.g. "zx", "ygo"
        tcg:       our canonical TCG identifier, e.g. "zx", "yugioh"
        delay:     seconds to sleep between HTTP requests
    """

    shop = "yuyutei"

    def __init__(self, game_code: str, tcg: str, delay: float = 1.0):
        self.game_code = game_code
        self.tcg = tcg
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._sets: list[dict] = []

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _get(self, url: str) -> BeautifulSoup:
        import time
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        time.sleep(self.delay)
        return BeautifulSoup(resp.text, "lxml")

    # ------------------------------------------------------------------
    # Set discovery
    # ------------------------------------------------------------------

    def _parse_sets_from_soup(self, soup: BeautifulSoup) -> list[dict]:
        """Extract all sets from the sidebar of any yuyu-tei page."""
        pattern = re.compile(rf"^side-sell-{re.escape(self.game_code)}-s-(.+)$")
        sets: list[dict] = []
        seen: set[str] = set()

        for btn in soup.find_all("button", id=pattern):
            m = pattern.match(btn.get("id", ""))
            if not m:
                continue
            set_code_lower = m.group(1)            # e.g. "b01"
            set_code = set_code_lower.upper()       # e.g. "B01"
            if set_code in seen:
                continue
            seen.add(set_code)

            label = btn.get_text(strip=True)        # e.g. "[B01] 異世界との邂逅"
            set_name = re.sub(r"^\[.*?\]\s*", "", label).strip()

            sets.append({
                "set_code": set_code,
                "set_code_lower": set_code_lower,
                "set_name": set_name,
                "url": f"{YUYUTEI_BASE}/sell/{self.game_code}/s/{set_code_lower}",
            })

        return sets

    def fetch_sets(self) -> list[dict]:
        """Fetch and cache all set metadata from the top page."""
        if self._sets:
            return self._sets
        logger.info("Fetching set list from yuyu-tei top page (game=%s)", self.game_code)
        soup = self._get(f"{YUYUTEI_BASE}/top/{self.game_code}")
        self._sets = self._parse_sets_from_soup(soup)
        logger.info("Found %d sets", len(self._sets))
        return self._sets

    # ------------------------------------------------------------------
    # Card parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_price(text: str) -> float | None:
        m = _PRICE_RE.search(text)
        return float(m.group().replace(",", "")) if m else None

    @staticmethod
    def _parse_quantity(card_div: Tag) -> int:
        """Read stock quantity from the cart_limit hidden input.

        0 means sold out; positive N means N copies available.
        """
        inp = card_div.find("input", class_="cart_limit")
        if inp:
            try:
                return int(inp.get("value", 0))
            except (ValueError, TypeError):
                pass
        # Fallback: parse the label text
        label = card_div.find("label", class_="cart_sell_zaiko")
        if label:
            txt = label.get_text(strip=True)
            m = re.search(r"(\d+)\s*点", txt)
            if m:
                return int(m.group(1))
            if "×" in txt:
                return 0
        return 0

    def _parse_card_div(
        self,
        card_div: Tag,
        rarity_raw: str,
        set_code: str,
        set_url: str,
    ) -> ShopListing | None:
        """Parse one div.card-product into a ShopListing."""
        # Card number ─ <span class="d-block border border-dark ...">B01-101</span>
        cardno_span = card_div.find(
            "span", class_=lambda c: c and "border-dark" in c
        )
        if not cardno_span:
            return None
        card_number_raw = cardno_span.get_text(strip=True)

        # Card name ─ <h4 class="text-primary fw-bold">名前</h4>
        name_h4 = card_div.find("h4", class_=lambda c: c and "text-primary" in c)
        card_name_raw = name_h4.get_text(strip=True) if name_h4 else ""

        # Price ─ <strong class="d-block text-end">220 円</strong>
        price_strong = card_div.find("strong", class_=lambda c: c and "d-block" in c)
        price = self._parse_price(price_strong.get_text(strip=True)) if price_strong else None
        if price is None:
            return None

        # Stock quantity
        quantity = self._parse_quantity(card_div)
        is_sold_out = "sold-out" in card_div.get("class", [])

        # Card detail URL ─ href may be absolute or relative
        link = card_div.find("a", href=re.compile(r"/sell/.+/card/"))
        if link:
            href = link["href"]
            card_url = href if href.startswith("http") else f"{YUYUTEI_BASE}{href}"
        else:
            card_url = set_url

        # Internal product ID (for future reference)
        cid_input = card_div.find("input", class_="cart_cid")
        product_id = cid_input.get("value") if cid_input else None

        return ShopListing(
            shop=self.shop,
            tcg=self.tcg,
            set_code=set_code,
            card_number_raw=card_number_raw,
            card_name_raw=card_name_raw,
            rarity_raw=rarity_raw,
            condition="NM",       # yuyu-tei sells NM singles by default
            price=price,
            currency="JPY",
            quantity=quantity,
            url=card_url,
            crawled_at=datetime.now(timezone.utc),
            extra={
                "product_id": product_id,
                "is_sold_out": is_sold_out,
                "game_code": self.game_code,
            },
        )

    def _iter_set_page(
        self, soup: BeautifulSoup, set_code: str, set_url: str
    ) -> Iterator[ShopListing]:
        """Yield all listings from a parsed set page."""
        for section in soup.find_all("div", class_="cards-list"):
            card_divs = section.find_all("div", class_="card-product")
            if not card_divs:
                continue

            # Rarity label ─ span.text-white inside the h3 heading
            rarity_raw = ""
            h3 = section.find("h3")
            if h3:
                rarity_span = h3.find(
                    "span", class_=lambda c: c and "text-white" in c
                )
                if rarity_span:
                    rarity_raw = rarity_span.get_text(strip=True)
                else:
                    # Fallback: take text before "Card List"
                    rarity_raw = h3.get_text(strip=True).split("Card")[0].strip()

            for card_div in card_divs:
                listing = self._parse_card_div(card_div, rarity_raw, set_code, set_url)
                if listing:
                    yield listing

    # ------------------------------------------------------------------
    # ShopCrawler interface
    # ------------------------------------------------------------------

    def crawl_set(self, set_code: str) -> Iterator[ShopListing]:
        """Crawl all listings for one set (all rarities, one page).

        Args:
            set_code: uppercase set code, e.g. "B01"
        """
        set_code_lower = set_code.lower()
        url = f"{YUYUTEI_BASE}/sell/{self.game_code}/s/{set_code_lower}"
        logger.info("Crawling set %s — %s", set_code, url)
        soup = self._get(url)
        # Opportunistically populate the set list from the sidebar
        if not self._sets:
            self._sets = self._parse_sets_from_soup(soup)
        yield from self._iter_set_page(soup, set_code, url)

    def search_card(self, card_number: str) -> Iterator[ShopListing]:
        """yuyu-tei is indexed per-set; cross-set card search is not supported."""
        raise NotImplementedError(
            "yuyu-tei is crawled per-set. Use crawl_set() with the appropriate set code."
        )

    # ------------------------------------------------------------------
    # Full crawl orchestration
    # ------------------------------------------------------------------

    def _fetch_sets_from_db(self, conn) -> list[dict]:
        """Fall back to set codes already stored in the game-specific DB table.

        For ZX, reads zx_sets; for other TCGs returns an empty list.
        """
        table_map = {"zx": "zx_sets"}
        table = table_map.get(self.game_code)
        if not table:
            return []
        try:
            rows = conn.execute(
                f"SELECT set_code, set_name FROM {table} ORDER BY set_code"
            ).fetchall()
        except Exception:
            return []
        sets = [
            {
                "set_code": row[0],
                "set_code_lower": row[0].lower(),
                "set_name": row[1] or "",
                "url": f"{YUYUTEI_BASE}/sell/{self.game_code}/s/{row[0].lower()}",
            }
            for row in rows
        ]
        logger.info(
            "Loaded %d sets from DB table %s (top-page discovery unavailable)",
            len(sets), table,
        )
        return sets

    def run_full_crawl(self, db_path=None) -> None:
        """Crawl all sets and persist listings to DuckDB.

        Skips sets whose listings were already crawled today.
        """
        conn = get_connection(db_path or DB_PATH)
        init_schema(conn)
        _init_yuyutei_schema(conn)

        sets = self.fetch_sets()
        if not sets:
            # Top page no longer lists all sets — fall back to zx_sets table when available
            sets = self._fetch_sets_from_db(conn)
        if not sets:
            logger.error("No sets discovered — check game_code=%s", self.game_code)
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

        to_crawl = [s for s in sets if s["set_code"] not in crawled_today]
        logger.info(
            "%d/%d sets already crawled today, crawling %d remaining",
            len(crawled_today), len(sets), len(to_crawl),
        )

        for set_info in to_crawl:
            set_code = set_info["set_code"]
            logger.info("→ %s (%s)", set_code, set_info["set_name"])
            batch: list[dict] = []
            count = 0

            try:
                for listing in self.crawl_set(set_code):
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
                """INSERT OR REPLACE INTO yuyutei_sets
                       (game_code, set_code, set_name, last_crawled_at, listing_count)
                   VALUES (?, ?, ?, now(), ?)""",
                [self.game_code, set_code, set_info["set_name"], count],
            )
            logger.info("   saved %d listings", count)

        conn.close()
        logger.info("YuYuTei full crawl complete (game=%s)", self.game_code)
