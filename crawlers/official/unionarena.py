"""Union Arena TCG official card list crawler — unionarena-tcg.com (Japanese).

URL structure
-------------
  Entry / search : GET  https://www.unionarena-tcg.com/jp/cardlist/?search=true
  Card search    : POST https://www.unionarena-tcg.com/jp/cardlist/index.php?search=true
                   body: series=NNN
                   All cards for a series are embedded in one HTML response;
                   JavaScript handles page-switching client-side — no HTTP pagination.
  Card detail    : GET  https://www.unionarena-tcg.com/jp/cardlist/detail_iframe.php
                         ?card_no=UA01BT/CGH-1-001
                   Required for: rarity, BP, AP, card type, attribute, effects.

HTML structure — series select (on /jp/cardlist/?search=true)
--------------------------------------------------------------
  <select name="series">
    <option value="570101">コードギアス 反逆のルルーシュ 【UA01BT】</option>
    …
  </select>
  <select name="selectTitle">
    <option value="コードギアス 反逆のルルーシュ">コードギアス 反逆のルルーシュ</option>
    …
  </select>

HTML structure — card list (ul.cardlistCol > li.cardImgCol)
-----------------------------------------------------------
  <li class="cardImgCol">
    <a href="./detail_iframe.php?card_no=UA01BT/CGH-1-001">
      <img alt="UA01BT/CGH-1-001 〇 ルルーシュ"
           data-src="/jp/images/cardlist/card/UA01BT_CGH-1-001.png?v7"/>
    </a>
  </li>
  <!-- Parallel variant: card_no ends with _p1, _p2, … -->
  <li class="cardImgCol">
    <a href="./detail_iframe.php?card_no=UA01BT/CGH-1-004_p1">…</a>
  </li>

HTML structure — card detail iframe
------------------------------------
  <h2 class="cardNameCol">ルルーシュ
    <span class="rubyData">ルルーシュ・ランペルージ</span>
  </h2>
  <div class="cardNumCol">
    <span class="cardNumData">UA01BT/CGH-1-001</span>
    <span class="rareData">U</span>
  </div>
  <dd class="cardDataTitleCol cgh">  <!-- title logo; alt = franchise name -->
    <img alt="コードギアス 反逆のルルーシュ"/>
  </dd>
  <!-- card stats in dl.cardDataCol elements -->
  <dl class="cardDataCol needEnergyData">…</dl>
  <dl class="cardDataCol apData"><dd>1</dd></dl>
  <dl class="cardDataCol categoryData"><dd>キャラクター</dd></dl>
  <dl class="cardDataCol bpData"><dd>2500</dd></dl>
  <dl class="cardDataCol attributeData"><dd>ブリタニア帝国</dd></dl>
  <dl class="cardDataCol generatedEnergyData">…</dl>
  <dl class="cardDataCol effectData"><dd>…</dd></dl>
  <dl class="cardDataCol triggerData"><dd>-</dd></dl>

Key design decisions
---------------------
  numbering_scheme : "unique_per_rarity" — each card_number is unique. Parallel variants
                     carry a _p1/_p2 suffix in the official card_no making them distinct.
  card_base_id     : card_no with the _p* suffix stripped (base card number).
  set_code         : series code extracted from the series option text 【UA01BT】.
                     For "NEW CARD SELECTION" series (no brackets), the full series
                     option text is used.
  title connection : ua_sets stores the title_name (franchise) for each set.
                     ua_titles stores all known franchise titles.
  --set argument   : accepts the numeric series_id (e.g. --set 570101).
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

UA_BASE = "https://www.unionarena-tcg.com"
CARDLIST_URL = f"{UA_BASE}/jp/cardlist/"
SEARCH_URL = f"{UA_BASE}/jp/cardlist/index.php?search=true"
DETAIL_URL = f"{UA_BASE}/jp/cardlist/detail_iframe.php"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}

# Extracts set_code from series option text: "コードギアス 反逆のルルーシュ 【UA01BT】"
_SET_CODE_RE = re.compile(r"【([^】]+)】")
# Parallel suffix: UA01BT/CGH-1-004_p1 → "_p1"
_PARALLEL_RE = re.compile(r"(_p\d+)$", re.IGNORECASE)


@dataclass
class UASet:
    series_id: int
    set_code: str       # e.g. "UA01BT"
    set_name: str       # full option text
    title_name: str     # franchise name e.g. "コードギアス 反逆のルルーシュ"


def _init_unionarena_schema(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ua_titles (
            title_name  VARCHAR PRIMARY KEY,
            crawled_at  TIMESTAMPTZ
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ua_sets (
            series_id   INTEGER PRIMARY KEY,
            set_code    VARCHAR,
            set_name    VARCHAR,
            title_name  VARCHAR,
            total_cards INTEGER,
            crawled_at  TIMESTAMPTZ
        );
    """)


def _parse_series_option(text: str, title_values: list[str]) -> tuple[str, str]:
    """Return (set_code, title_name) from a series option text."""
    m = _SET_CODE_RE.search(text)
    if m:
        set_code = m.group(1).strip()
        # Remove 【...】 and Vol.N from text to get title part
        title_part = _SET_CODE_RE.sub("", text).strip()
        title_part = re.sub(r"\bVol\.\d+\b", "", title_part).strip()
        # Match against known title values (longest prefix match)
        best = ""
        for tv in title_values:
            if tv and (title_part == tv or title_part.startswith(tv)):
                if len(tv) > len(best):
                    best = tv
        return set_code, (best or title_part)
    else:
        # "NEW CARD SELECTION ..." — no bracket code
        return text.strip(), text.strip()


def _strip_parallel(card_no: str) -> tuple[str, str]:
    """Split 'UA01BT/CGH-1-004_p1' into ('UA01BT/CGH-1-004', '_p1')."""
    m = _PARALLEL_RE.search(card_no)
    if m:
        suffix = m.group(1)
        base = card_no[: -len(suffix)]
        return base, suffix
    return card_no, ""


class UnionArenaOfficialCrawler(OfficialCrawler):
    """Official card crawler for the Japanese Union Arena TCG card database."""

    tcg = "unionarena"

    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._sets: list[UASet] = []
        self._titles: list[str] = []

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
    # Set discovery
    # ------------------------------------------------------------------

    def crawl_sets(self) -> Iterator[UASet]:
        if not self._sets:
            self._sets, self._titles = self._fetch_sets()
        yield from self._sets

    def _fetch_sets(self) -> tuple[list[UASet], list[str]]:
        logger.info("Fetching Union Arena set list")
        soup = self._get(CARDLIST_URL, params={"search": "true"})

        title_sel = soup.find("select", {"name": "selectTitle"})
        series_sel = soup.find("select", {"name": "series"})

        title_values: list[str] = []
        if title_sel:
            title_values = [
                opt.get("value", "")
                for opt in title_sel.find_all("option")
                if opt.get("value", "").strip()
            ]

        sets: list[UASet] = []
        if series_sel:
            for opt in series_sel.find_all("option"):
                val = opt.get("value", "").strip()
                if not val or not val.isdigit():
                    continue
                text = opt.get_text(strip=True)
                set_code, title_name = _parse_series_option(text, title_values)
                sets.append(UASet(
                    series_id=int(val),
                    set_code=set_code,
                    set_name=text,
                    title_name=title_name,
                ))

        logger.info("Found %d sets across %d titles", len(sets), len(title_values))
        return sets, title_values

    # ------------------------------------------------------------------
    # Card list for a series (one POST, all cards in HTML)
    # ------------------------------------------------------------------

    def _fetch_card_list(self, series_id: int) -> list[dict]:
        """Return [{card_no, image_url}] for all cards in the series."""
        soup = self._post(SEARCH_URL, data={"series": str(series_id)})
        items = soup.select("li.cardImgCol")
        cards = []
        for li in items:
            a = li.find("a")
            img = li.find("img")
            if not a:
                continue
            href = a.get("href", "")
            if "card_no=" not in href:
                continue
            card_no = href.split("card_no=", 1)[1]
            src = img.get("data-src", "") if img else ""
            image_url = f"{UA_BASE}{src}" if src.startswith("/") else src
            cards.append({"card_no": card_no, "image_url": image_url})
        return cards

    # ------------------------------------------------------------------
    # Card detail (per-card iframe)
    # ------------------------------------------------------------------

    def _fetch_card_detail(self, card_no: str) -> dict:
        soup = self._get(DETAIL_URL, params={"card_no": card_no})

        # Card name (strip ruby reading)
        name_h2 = soup.find("h2", class_="cardNameCol")
        card_name = ""
        if name_h2:
            ruby = name_h2.find("span", class_="rubyData")
            if ruby:
                ruby.decompose()
            card_name = name_h2.get_text(strip=True)

        # Official card number + rarity
        num_el = soup.find("span", class_="cardNumData")
        official_no = num_el.get_text(strip=True) if num_el else card_no
        rare_el = soup.find("span", class_="rareData")
        rarity = rare_el.get_text(strip=True) if rare_el else ""

        # Title from title logo img alt
        title_dd = soup.find("dd", class_="cardDataTitleCol")
        franchise = ""
        if title_dd:
            logo_img = title_dd.find("img")
            if logo_img:
                franchise = logo_img.get("alt", "")

        # Image URL from cardDataImgCol
        img_url = ""
        img_dd = soup.find("dd", class_="cardDataImgCol")
        if img_dd:
            img_tag = img_dd.find("img")
            if img_tag:
                src = img_tag.get("src", "")
                img_url = f"{UA_BASE}{src}" if src.startswith("/") else src

        # Card stats from dl.cardDataCol elements — keyed by CSS class suffix
        def _dl_text(css_class: str) -> str:
            dl = soup.find("dl", class_=css_class)
            if not dl:
                return ""
            dd = dl.find("dd")
            return dd.get_text(strip=True) if dd else ""

        def _dl_img_alt(css_class: str) -> str:
            """For energy fields represented as images."""
            dl = soup.find("dl", class_=css_class)
            if not dl:
                return ""
            imgs = dl.find_all("img")
            return ",".join(i.get("alt", "") for i in imgs if i.get("alt", ""))

        return {
            "official_no": official_no,
            "rarity": rarity,
            "card_name": card_name,
            "franchise": franchise,
            "image_url": img_url,
            "ap": _dl_text("apData"),
            "bp": _dl_text("bpData"),
            "card_type": _dl_text("categoryData"),
            "attribute": _dl_text("attributeData"),
            "need_energy": _dl_img_alt("needEnergyData"),
            "generated_energy": _dl_img_alt("generatedEnergyData"),
            "effect": _dl_text("effectData"),
            "trigger": _dl_text("triggerData"),
        }

    # ------------------------------------------------------------------
    # OfficialCrawler interface
    # ------------------------------------------------------------------

    def crawl_cards(self, set_code: str) -> Iterator[OfficialCard]:
        """Crawl cards for a series. Pass numeric series_id as set_code (e.g. '570101')."""
        list(self.crawl_sets())
        if not set_code.isdigit():
            logger.error(
                "unionarena-official --set requires a numeric series_id (e.g. --set 570101)"
            )
            return
        matched = [s for s in self._sets if s.series_id == int(set_code)]
        if not matched:
            logger.warning("Series id %s not found", set_code)
            return
        yield from self._crawl_series(matched[0])

    def _crawl_series(self, ua_set: UASet) -> Iterator[OfficialCard]:
        logger.info("Crawling series %d (%s) — %s", ua_set.series_id, ua_set.set_code, ua_set.set_name)

        card_list = self._fetch_card_list(ua_set.series_id)
        logger.info("  %d card entries in list", len(card_list))

        for entry in tqdm(card_list, desc=ua_set.set_code, unit="card", leave=False):
            card_no = entry["card_no"]
            base_no, parallel_suffix = _strip_parallel(card_no)

            try:
                detail = self._fetch_card_detail(card_no)
            except Exception:
                logger.warning("  Failed to fetch detail for %s — skipping", card_no)
                continue

            rarity_code = detail["rarity"] + parallel_suffix if parallel_suffix else detail["rarity"]
            # Use detail image if available, else fall back to list thumbnail
            image_url = detail["image_url"] or entry["image_url"]

            yield OfficialCard(
                tcg=self.tcg,
                set_code=ua_set.set_code,
                set_name=ua_set.set_name,
                card_number=card_no,
                card_name=detail["card_name"],
                rarity_code=rarity_code,
                rarity_name=detail["rarity"],
                numbering_scheme="unique_per_rarity",
                card_base_id=base_no,
                extra={
                    "series_id": ua_set.series_id,
                    "title_name": ua_set.title_name,
                    "franchise": detail["franchise"],
                    "parallel_suffix": parallel_suffix,
                    "ap": detail["ap"],
                    "bp": detail["bp"],
                    "card_type": detail["card_type"],
                    "attribute": detail["attribute"],
                    "need_energy": detail["need_energy"],
                    "generated_energy": detail["generated_energy"],
                    "effect": detail["effect"],
                    "trigger": detail["trigger"],
                    "image_url": image_url,
                },
            )

    # ------------------------------------------------------------------
    # Full crawl
    # ------------------------------------------------------------------

    def run_full_crawl(self, db_path=None) -> None:
        conn = get_connection(db_path or DB_PATH)
        init_schema(conn)
        _init_unionarena_schema(conn)

        all_sets, all_titles = self._fetch_sets()
        if not all_sets:
            logger.error("No sets found — check network connection")
            return

        # Persist all known titles
        conn.executemany(
            "INSERT OR IGNORE INTO ua_titles (title_name) VALUES (?)",
            [(t,) for t in all_titles],
        )

        done_ids: set[int] = {
            r[0] for r in conn.execute(
                "SELECT series_id FROM ua_sets WHERE crawled_at IS NOT NULL"
            ).fetchall()
        }

        to_crawl = [s for s in all_sets if s.series_id not in done_ids]
        logger.info(
            "%d/%d series already crawled, crawling %d remaining",
            len(done_ids), len(all_sets), len(to_crawl),
        )

        for ua_set in tqdm(to_crawl, desc="Series", unit="set"):
            batch: list[dict] = []
            count = 0

            try:
                for card in self._crawl_series(ua_set):
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
                logger.exception("Failed to crawl series %d — skipping", ua_set.series_id)
                continue

            if batch:
                insert_official_cards(conn, batch)

            conn.execute(
                """INSERT OR REPLACE INTO ua_sets
                       (series_id, set_code, set_name, title_name, total_cards, crawled_at)
                   VALUES (?, ?, ?, ?, ?, now())""",
                [ua_set.series_id, ua_set.set_code, ua_set.set_name, ua_set.title_name, count],
            )
            logger.info("  saved %d cards for series %d (%s)", count, ua_set.series_id, ua_set.set_code)

        conn.close()
        logger.info("Union Arena full crawl complete")
