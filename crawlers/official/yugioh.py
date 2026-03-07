"""Yu-Gi-Oh! official card crawler — db.yugioh-card.com (Japanese OCG).

URL structure
-------------
  Set list  : GET https://www.db.yugioh-card.com/yugiohdb/card_list
  Card list  : GET https://www.db.yugioh-card.com/yugiohdb/card_search.action?ope=1&sess=1&pid={pid}&rp=99999
  Card detail: GET https://www.db.yugioh-card.com/yugiohdb/card_search.action?ope=2&cid={cid}

HTML structure — set list page (card_list)
-------------------------------------------
  <div class="t_row packc1 packsub128 packc1_packsub128">
    <div class="inside">
      <div class="sub">
        <div class="time">2023-10-28</div>
        <div class="catergory">
          <span class="ws_nowrap">【基本ブースターパック】</span>
          <span class="ws_nowrap">2023年4月～</span>
        </div>
      </div>
      <div class="main">
        <p>ファントム・ナイトメア [ PHANTOM NIGHTMARE ]</p>
        <input class="link_value" type="hidden"
               value="/yugiohdb/card_search.action?ope=1&sess=1&pid=1000009002000&rp=99999"/>
      </div>
    </div>
  </div>

HTML structure — card list page (ope=1)
-----------------------------------------
  <div class="t_row c_normal t_rid_3">
    <span class="card_name">スピリット・オブ・ユベル</span>
    <input class="cid" type="hidden" value="19456"/>
  </div>

HTML structure — card detail page (ope=2)
-------------------------------------------
  <div class="t_row">
    <div class="inside">
      <div class="time">2023-10-28</div>
      <div class="flex_1 contents">
        <div class="card_number">PHNI-JP001</div>
        <div class="pack_name">ファントム・ナイトメア [ PHANTOM NIGHTMARE ]</div>
        <p>SR</p>
        <span>スーパーレア</span>
        <input class="link_value" type="hidden"
               value="/yugiohdb/card_search.action?ope=1&sess=1&pid=1000009002000&rp=99999"/>
      </div>
    </div>
  </div>

Key design decisions
---------------------
  numbering_scheme : "shared_official" — same card_number for all rarities in a set.
                     E.g. PHNI-JP001 can be SR, SE, or QCSE; each is a separate OfficialCard row.
  card_base_id     : cid (YuGiOh DB's cross-set card identifier)
  Detail cache     : keyed by cid; stores all set appearances so cards encountered in
                     multiple sets are only fetched once.
"""

import json
import logging
import re
import time
from typing import Iterator

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from crawlers.official.base import OfficialCard, OfficialCrawler
from crawlers.storage import (
    DB_PATH, get_connection, init_schema, insert_official_cards,
)

logger = logging.getLogger(__name__)

YUGIOH_BASE = "https://www.db.yugioh-card.com"
CARD_LIST_URL = f"{YUGIOH_BASE}/yugiohdb/card_list"
CARD_SEARCH_URL = f"{YUGIOH_BASE}/yugiohdb/card_search.action"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,zh-CN;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    ),
}

# JP card number: 2-8 uppercase alphanumeric chars + -JP + 3-4 digits + optional letter
_CARD_NO_RE = re.compile(r"^([A-Z0-9]{2,8})-JP\d{3,4}[A-Z]?$")
_PID_RE = re.compile(r"pid=(\d+)")


def _init_yugioh_schema(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS yugioh_sets (
            pid          VARCHAR PRIMARY KEY,
            set_code     VARCHAR,
            set_name     VARCHAR,
            release_date VARCHAR,
            category     VARCHAR,
            total_cards  INTEGER,
            crawled_at   TIMESTAMPTZ
        );
    """)


class YugiohOfficialCrawler(OfficialCrawler):
    """Official card crawler for the Japanese YuGiOh OCG database.

    Crawl flow per set:
      1. Fetch card list page → get list of (cid, card_name)
      2. For each cid not yet cached: fetch detail page → store all JP set
         appearances keyed by pid
      3. Match appearances to current set's pid → yield one OfficialCard
         per (card_number, rarity_code) found
    """

    tcg = "yugioh"

    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        # Cache: cid -> list of {pid, card_number, rarity_code, rarity_name, pack_name}
        self._detail_cache: dict[str, list[dict]] = {}
        self._sets: list[dict] = []

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> BeautifulSoup:
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        time.sleep(self.delay)
        return BeautifulSoup(resp.text, "lxml")

    # ------------------------------------------------------------------
    # Set discovery
    # ------------------------------------------------------------------

    def crawl_sets(self) -> Iterator[dict]:
        if not self._sets:
            self._sets = self._fetch_sets()
        yield from self._sets

    def _fetch_sets(self) -> list[dict]:
        logger.info("Fetching set list from yugioh card_list page")
        soup = self._get(CARD_LIST_URL)
        sets = []
        for row in soup.find_all("div", class_="t_row"):
            link_input = row.find("input", class_="link_value")
            if not link_input:
                continue
            href = link_input.get("value", "")
            pid_m = _PID_RE.search(href)
            if not pid_m:
                continue
            pid = pid_m.group(1)

            inside = row.find("div", class_="inside")
            if not inside:
                continue

            main_div = inside.find("div", class_="main")
            name_p = main_div.find("p") if main_div else None
            set_name = name_p.get_text(strip=True) if name_p else ""

            sub_div = inside.find("div", class_="sub")
            date_el = sub_div.find("div", class_="time") if sub_div else None
            release_date = date_el.get_text(strip=True) if date_el else ""
            cat_spans = sub_div.find_all("span", class_="ws_nowrap") if sub_div else []
            category = " ".join(s.get_text(strip=True) for s in cat_spans)

            row_classes = row.get("class", [])
            packc_class = next((c for c in row_classes if re.match(r"^packc\d+$", c)), "")

            sets.append({
                "pid": pid,
                "set_name": set_name,
                "release_date": release_date,
                "category": category,
                "packc": packc_class,
                "card_list_url": f"{YUGIOH_BASE}{href}",
            })

        logger.info("Found %d sets", len(sets))
        return sets

    # ------------------------------------------------------------------
    # Card list for a set
    # ------------------------------------------------------------------

    def _fetch_card_list(self, set_info: dict) -> list[dict]:
        """Return list of {cid, card_name} for all cards in a set."""
        url = set_info["card_list_url"]
        soup = self._get(url)
        cards = []
        for row in soup.find_all("div", class_="t_row"):
            cid_input = row.find("input", class_="cid")
            if not cid_input:
                continue
            cid = cid_input.get("value", "").strip()
            if not cid:
                continue
            name_el = row.find("span", class_="card_name")
            card_name = name_el.get_text(strip=True) if name_el else ""
            cards.append({"cid": cid, "card_name": card_name})
        return cards

    # ------------------------------------------------------------------
    # Card detail (with cid-level caching)
    # ------------------------------------------------------------------

    def _fetch_detail(self, cid: str) -> list[dict]:
        """Fetch and cache a card's full JP set-appearance list."""
        if cid in self._detail_cache:
            return self._detail_cache[cid]

        soup = self._get(CARD_SEARCH_URL, params={"ope": "2", "cid": cid})

        # Card image URL is in the og:image meta tag (includes a signed enc token)
        og_image = soup.find("meta", property="og:image")
        image_url = og_image.get("content", "") if og_image else ""

        appearances = []
        for row in soup.find_all("div", class_="t_row"):
            link_input = row.find("input", class_="link_value")
            if not link_input:
                continue
            href = link_input.get("value", "")
            pid_m = _PID_RE.search(href)
            if not pid_m:
                continue

            card_no_el = row.find("div", class_="card_number")
            card_number = card_no_el.get_text(strip=True) if card_no_el else ""
            if not _CARD_NO_RE.match(card_number):
                continue  # skip non-JP entries

            pack_el = row.find("div", class_="pack_name")
            pack_name = pack_el.get_text(strip=True) if pack_el else ""

            rarity_p = row.find("p")
            rarity_code = rarity_p.get_text(strip=True) if rarity_p else ""

            rarity_span = row.find("span")
            rarity_name = rarity_span.get_text(strip=True) if rarity_span else ""

            appearances.append({
                "pid": pid_m.group(1),
                "card_number": card_number,
                "rarity_code": rarity_code,
                "rarity_name": rarity_name,
                "pack_name": pack_name,
                "image_url": image_url,
            })

        self._detail_cache[cid] = appearances
        return appearances

    # ------------------------------------------------------------------
    # OfficialCrawler interface
    # ------------------------------------------------------------------

    def crawl_cards(self, set_code: str) -> Iterator[OfficialCard]:
        """Crawl cards for a set identified by its pid."""
        sets = list(self.crawl_sets())
        matched = [s for s in sets if s["pid"] == set_code]
        if not matched:
            logger.warning("Set pid %s not found", set_code)
            return
        yield from self._crawl_set(matched[0])

    def _crawl_set(self, set_info: dict) -> Iterator[OfficialCard]:
        pid = set_info["pid"]
        set_name = set_info["set_name"]
        logger.info("Crawling pid=%s  %s", pid, set_name[:50])

        card_list = self._fetch_card_list(set_info)
        logger.info("  %d cards in card list", len(card_list))

        for card in tqdm(card_list, desc=set_name[:40], unit="card", leave=False):
            cid = card["cid"]
            card_name = card["card_name"]

            appearances = self._fetch_detail(cid)
            matches = [a for a in appearances if a["pid"] == pid]

            if not matches:
                logger.debug("  No JP entry for cid=%s (%s) in pid=%s", cid, card_name, pid)
                continue

            for app in matches:
                card_number = app["card_number"]
                set_code_prefix = _CARD_NO_RE.match(card_number).group(1)

                yield OfficialCard(
                    tcg=self.tcg,
                    set_code=set_code_prefix,
                    set_name=set_name,
                    card_number=card_number,
                    card_name=card_name,
                    rarity_code=app["rarity_code"],
                    rarity_name=app["rarity_name"],
                    numbering_scheme="shared_official",
                    card_base_id=cid,
                    extra={
                        "pid": pid,
                        "cid": cid,
                        "pack_name": app["pack_name"],
                        "packc": set_info.get("packc", ""),
                        "release_date": set_info.get("release_date", ""),
                        "image_url": app.get("image_url", ""),
                    },
                )

    # ------------------------------------------------------------------
    # Full crawl
    # ------------------------------------------------------------------

    def run_full_crawl(self, db_path=None) -> None:
        """Crawl all sets and persist to DuckDB, skipping already-crawled sets."""
        conn = get_connection(db_path or DB_PATH)
        init_schema(conn)
        _init_yugioh_schema(conn)

        all_sets = self._fetch_sets()
        if not all_sets:
            logger.error("No sets found — check network connection")
            return

        done_pids: set[str] = {
            r[0] for r in conn.execute(
                "SELECT pid FROM yugioh_sets WHERE crawled_at IS NOT NULL"
            ).fetchall()
        }

        to_crawl = [s for s in all_sets if s["pid"] not in done_pids]
        logger.info(
            "%d/%d sets already crawled, crawling %d remaining",
            len(done_pids), len(all_sets), len(to_crawl),
        )

        for set_info in tqdm(to_crawl, desc="Sets", unit="set"):
            pid = set_info["pid"]
            batch: list[dict] = []
            count = 0

            try:
                for card in self._crawl_set(set_info):
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
                logger.exception("Failed to crawl set pid=%s — skipping", pid)
                continue

            if batch:
                insert_official_cards(conn, batch)

            conn.execute(
                """INSERT OR REPLACE INTO yugioh_sets
                       (pid, set_code, set_name, release_date, category,
                        total_cards, crawled_at)
                   VALUES (?, ?, ?, ?, ?, ?, now())""",
                [pid, None, set_info["set_name"],
                 set_info["release_date"], set_info["category"], count],
            )
            logger.info("  saved %d card editions (cache size: %d unique cards)",
                        count, len(self._detail_cache))

        conn.close()
        logger.info("YuGiOh full crawl complete")
