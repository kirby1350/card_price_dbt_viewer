"""Weiss Schwarz official card list crawler — ws-tcg.com (Japanese).

URL structure
-------------
  Expansion list : GET  https://ws-tcg.com/cardlist/
  Card search    : POST https://ws-tcg.com/cardlist/search
                   body: _method=POST&cmd=search&expansion=NNN
  Pagination     : GET  https://ws-tcg.com/cardlist/search?page=N
                   (POST sets search context; subsequent GET pages navigate it)

HTML structure — card search results (table.search-result-table)
-----------------------------------------------------------------
  <tr>
    <th>
      <a href="/cardlist/?cardno=IM/S07-001">
        <img src="/wordpress/wp-content/images/cardlist/i/im_s07/im_s07_001.png">
      </a>
    </th>
    <td>
      <h4>サイトウ真美 (IM/S07-001)</h4>
      <dl>
        <dt>サイド</dt><dd><img src="...schwarz.png"></dd>
        <dt>種類</dt><dd>キャラ</dd>
        <dt>レベル</dt><dd>0</dd>
        <dt>色</dt><dd><img src="...yellow.png"></dd>
        <dt>パワー</dt><dd>1500</dd>
        <dt>ソウル</dt><dd><img...></dd>
        <dt>コスト</dt><dd>0</dd>
        <dt>レアリティ</dt><dd>C</dd>
        <dt>トリガー</dt><dd>-</dd>
        <dt>特徴</dt><dd>音楽・アイドル</dd>
        <dt>フレーバー</dt><dd>...</dd>
      </dl>
      <p class="ability">...</p>
    </td>
  </tr>

Key design decisions
---------------------
  numbering_scheme : "unique_per_rarity" — SP variants carry a distinct card number
                     suffix (e.g. IM/S07-001 C and IM/S07-001S SR are separate rows).
  card_base_id     : card_number with any trailing letter suffix stripped, so
                     IM/S07-001S groups back to IM/S07-001.
  set_code         : prefix before the dash: "IM/S07-001" → "IM/S07".
  All card data is available on the search results page; no per-card detail fetches.
  Expansion IDs are passed as set_code when using --set (e.g. --set 29).
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

WS_BASE = "https://ws-tcg.com"
CARDLIST_URL = f"{WS_BASE}/cardlist/"
SEARCH_URL = f"{WS_BASE}/cardlist/search"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}

# IM/S07-001 or IM/S07-001S (SP variant) etc.
_CARD_NO_RE = re.compile(r"^([A-Za-z0-9]+/[A-Za-z0-9\-]+)-\d+[A-Za-z]?$")
# Strip trailing non-digit suffix from the number portion
_BASE_ID_RE = re.compile(r"^(.*-\d+)[A-Za-z]+$")


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


class WeissOfficialCrawler(OfficialCrawler):
    """Official card crawler for the Japanese Weiss Schwarz card database."""

    tcg = "weiss"

    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._expansions: list[WSExpansion] = []

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> BeautifulSoup:
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        time.sleep(self.delay)
        return BeautifulSoup(resp.text, "lxml")

    def _post(self, url: str, data: dict) -> BeautifulSoup:
        resp = self.session.post(url, data=data, timeout=30)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        time.sleep(self.delay)
        return BeautifulSoup(resp.text, "lxml")

    # ------------------------------------------------------------------
    # Expansion discovery
    # ------------------------------------------------------------------

    def crawl_sets(self) -> Iterator[WSExpansion]:
        if not self._expansions:
            self._expansions = self._fetch_expansions()
        yield from self._expansions

    def _fetch_expansions(self) -> list[WSExpansion]:
        logger.info("Fetching Weiss Schwarz expansion list")
        soup = self._get(CARDLIST_URL)
        select = soup.find("select", {"name": "expansion"})
        if not select:
            logger.error("Could not find expansion select element on %s", CARDLIST_URL)
            return []
        expansions = []
        for opt in select.find_all("option"):
            val = opt.get("value", "").strip()
            if not val or not val.isdigit():
                continue
            expansions.append(WSExpansion(
                expansion_id=int(val),
                set_name=opt.get_text(strip=True),
            ))
        logger.info("Found %d expansions", len(expansions))
        return expansions

    # ------------------------------------------------------------------
    # Card parsing (search results page)
    # ------------------------------------------------------------------

    def _parse_card_rows(self, soup: BeautifulSoup) -> list[dict]:
        table = soup.find("table", class_="search-result-table")
        if not table:
            return []
        tbody = table.find("tbody")
        if not tbody:
            return []

        rows = []
        for tr in tbody.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not td:
                continue

            # Image URL
            image_url = ""
            if th:
                img = th.find("img")
                if img:
                    src = img.get("src", "")
                    image_url = f"{WS_BASE}{src}" if src.startswith("/") else src

            # Card number + name from h4: "サイトウ真美 (IM/S07-001)"
            h4 = td.find("h4")
            card_number = ""
            card_name = ""
            if h4:
                text = h4.get_text(strip=True)
                m = re.search(r"\(([^)]+)\)\s*$", text)
                if m:
                    card_number = m.group(1).strip()
                    card_name = text[: m.start()].strip()
                else:
                    card_name = text

            # Fallback: card number from th link href
            if not card_number and th:
                a = th.find("a")
                if a:
                    href = a.get("href", "")
                    cm = re.search(r"cardno=([^&]+)", href)
                    if cm:
                        card_number = cm.group(1).strip()

            if not card_number:
                continue

            # Parse dl key→value pairs
            kv: dict[str, str] = {}
            dl = td.find("dl")
            if dl:
                dts = dl.find_all("dt")
                dds = dl.find_all("dd")
                for dt, dd in zip(dts, dds):
                    key = dt.get_text(strip=True)
                    imgs = dd.find_all("img")
                    if imgs:
                        # Represent image-based values by filename stem (side, color, soul, trigger)
                        parts = []
                        for img in imgs:
                            src = img.get("src", "")
                            stem_m = re.search(r"/([^/]+)\.\w+$", src)
                            parts.append(stem_m.group(1) if stem_m else "")
                        kv[key] = ",".join(parts)
                    else:
                        kv[key] = dd.get_text(strip=True)

            effect_el = td.find("p", class_="ability")
            effect = effect_el.get_text(strip=True) if effect_el else ""

            rows.append({
                "card_number": card_number,
                "card_name": card_name,
                "rarity": kv.get("レアリティ", ""),
                "side": kv.get("サイド", ""),
                "card_type": kv.get("種類", ""),
                "level": kv.get("レベル", ""),
                "color": kv.get("色", ""),
                "power": kv.get("パワー", ""),
                "soul": kv.get("ソウル", ""),
                "cost": kv.get("コスト", ""),
                "trigger": kv.get("トリガー", ""),
                "traits": kv.get("特徴", ""),
                "flavor": kv.get("フレーバー", ""),
                "effect": effect,
                "image_url": image_url,
            })
        return rows

    # ------------------------------------------------------------------
    # OfficialCrawler interface
    # ------------------------------------------------------------------

    def crawl_cards(self, set_code: str) -> Iterator[OfficialCard]:
        """Crawl cards for an expansion. Pass expansion_id as set_code (e.g. '29')."""
        list(self.crawl_sets())
        if not set_code.isdigit():
            logger.error(
                "weiss-official --set requires a numeric expansion_id (e.g. --set 29)"
            )
            return
        matched = [e for e in self._expansions if e.expansion_id == int(set_code)]
        if not matched:
            logger.warning("Expansion id %s not found", set_code)
            return
        yield from self._crawl_expansion(matched[0])

    def _crawl_expansion(self, exp: WSExpansion) -> Iterator[OfficialCard]:
        logger.info("Crawling expansion %d — %s", exp.expansion_id, exp.set_name)

        # Initiate search via POST
        soup = self._post(SEARCH_URL, data={
            "_method": "POST",
            "cmd": "search",
            "expansion": str(exp.expansion_id),
        })

        all_rows: list[dict] = []
        page = 1
        while True:
            rows = self._parse_card_rows(soup)
            if not rows:
                break
            all_rows.extend(rows)

            # Find next page link (<p class="pager"> … <span class="next"><a>)
            pager = soup.find("p", class_="pager")
            next_href = None
            if pager:
                next_span = pager.find("span", class_="next")
                if next_span:
                    a = next_span.find("a")
                    if a:
                        next_href = a.get("href", "")
            if not next_href:
                break

            page += 1
            next_url = f"{WS_BASE}{next_href}" if next_href.startswith("/") else next_href
            soup = self._get(next_url)

        logger.info("  %d cards found in %d page(s)", len(all_rows), page)

        for row in tqdm(all_rows, desc=exp.set_name[:40], unit="card", leave=False):
            card_number = row["card_number"]
            yield OfficialCard(
                tcg=self.tcg,
                set_code=_extract_set_code(card_number),
                set_name=exp.set_name,
                card_number=card_number,
                card_name=row["card_name"],
                rarity_code=row["rarity"],
                rarity_name=row["rarity"],
                numbering_scheme="unique_per_rarity",
                card_base_id=_card_base_id(card_number),
                extra={
                    "expansion_id": exp.expansion_id,
                    "side": row["side"],
                    "card_type": row["card_type"],
                    "level": row["level"],
                    "color": row["color"],
                    "power": row["power"],
                    "soul": row["soul"],
                    "cost": row["cost"],
                    "trigger": row["trigger"],
                    "traits": row["traits"],
                    "flavor": row["flavor"],
                    "effect": row["effect"],
                    "image_url": row["image_url"],
                },
            )

    # ------------------------------------------------------------------
    # Full crawl
    # ------------------------------------------------------------------

    def run_full_crawl(self, db_path=None) -> None:
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

        conn.close()
        logger.info("Weiss Schwarz full crawl complete")
