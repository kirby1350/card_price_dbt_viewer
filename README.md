# card_price_dbt_viewer

A card price tracking and visualization system for TCG cards. Crawlers scrape official card lists and shop prices, dbt transforms the raw data into analytics-ready models, and Evidence.dev renders a price dashboard.

**Supported TCGs:** Yu-Gi-Oh (OCG/JP), Z/X -Zillions of enemy X-, Cardfight!! Vanguard, Weiss Schwarz, Digimon Card Game, Union Arena

**Stack:** Python 3.12, DuckDB, dbt-duckdb, Evidence.dev

## Architecture

```
Official sites / Shops
        ↓ crawlers/
DuckDB raw tables (raw_official_cards, raw_shop_listings)
        ↓ dbt staging/
Cleaned, normalized views per source
        ↓ dbt intermediate/
int_card_editions        — canonical tradeable unit (tcg + card_number + rarity_code)
int_card_base            — logical card identity (groups editions across rarities)
int_shop_prices_matched  — shop listings matched to card_edition_id
        ↓ dbt marts/
mart_card_price_latest   — most recent price per edition per shop
mart_card_price_stats    — min/max/avg/median aggregates
mart_card_price_history  — daily snapshots for trend charts
        ↓ Evidence.dev
Dashboard pages querying mart_ tables
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running Crawlers

All crawlers are invoked via `main.py`:

```bash
# Official card lists
python main.py crawl zx-official               # all Z/X sets (~300 sets, several hours at 1.5s delay)
python main.py crawl zx-official --set B01     # single set (testing)
python main.py crawl yugioh-official           # all YuGiOh sets (~1,400 sets)
python main.py crawl yugioh-official --set <pid>
python main.py crawl vanguard-official         # all Vanguard expansions
python main.py crawl vanguard-official --set DZ-BT13
python main.py crawl weiss-official            # all Weiss Schwarz expansions
python main.py crawl weiss-official --set 29   # single expansion by numeric ID
python main.py crawl digimon-official          # all Digimon sets
python main.py crawl digimon-official --set 503036  # single set by numeric category ID
python main.py crawl unionarena-official       # all Union Arena series
python main.py crawl unionarena-official --set 570101  # single series by numeric ID

# Shop prices — YuYuTei
python main.py crawl yuyutei-zx                # YuYuTei Z/X listings
python main.py crawl yuyutei-ygo               # YuYuTei Yu-Gi-Oh listings

# Shop prices — Bigweb (API-based)
python main.py crawl bigweb-zx                 # Z/X
python main.py crawl bigweb-yugioh             # Yu-Gi-Oh
python main.py crawl bigweb-vanguard           # Cardfight!! Vanguard
python main.py crawl bigweb-weiss              # Weiss Schwarz
python main.py crawl bigweb-digimon            # Digimon Card Game
python main.py crawl bigweb-unionarena         # Union Arena
python main.py crawl bigweb-zx --set B01       # single set by set code

# Shop prices — Card Rush (HTML scraper)
python main.py crawl cardrush-ygo              # cardrush.jp Yu-Gi-Oh
python main.py crawl cardrush-vanguard         # cardrush-vanguard.jp
python main.py crawl cardrush-digimon          # cardrush-digimon.jp
python main.py crawl cardrush-digimon --set 189  # single product-group by numeric ID

# Options
python main.py crawl <target> --delay 2.0      # seconds between requests (default: 1.0)
python main.py crawl <target> --debug          # enable DEBUG logging
```

Raw data is stored in `data/raw.duckdb`. Each crawler skips sets that have already been successfully crawled.

### Running crawlers in parallel

DuckDB allows only one writer at a time. To run multiple official crawlers simultaneously, write each to a separate file then merge:

```bash
# Step 1 — launch each crawler with its own DB file (parallel terminals / background jobs)
python main.py crawl zx-official        --db data/raw_zx.duckdb
python main.py crawl yugioh-official    --db data/raw_yugioh.duckdb
python main.py crawl vanguard-official  --db data/raw_vanguard.duckdb
python main.py crawl weiss-official     --db data/raw_weiss.duckdb
python main.py crawl digimon-official   --db data/raw_digimon.duckdb
python main.py crawl unionarena-official --db data/raw_unionarena.duckdb
python main.py crawl bigweb-yugioh      --db data/raw_bigweb_yugioh.duckdb
python main.py crawl bigweb-vanguard    --db data/raw_bigweb_vanguard.duckdb
python main.py crawl bigweb-digimon     --db data/raw_bigweb_digimon.duckdb
python main.py crawl bigweb-unionarena  --db data/raw_bigweb_unionarena.duckdb
python main.py crawl cardrush-ygo       --db data/raw_cardrush_ygo.duckdb
python main.py crawl cardrush-vanguard  --db data/raw_cardrush_vanguard.duckdb
python main.py crawl cardrush-digimon   --db data/raw_cardrush_digimon.duckdb

# Step 2 — merge all raw_*.duckdb files into data/raw.duckdb
python main.py merge
```

The `merge` command attaches each source file read-only and uses `INSERT OR REPLACE` so it is safe to re-run. You can also specify files and target explicitly:

```bash
python main.py merge data/raw_zx.duckdb data/raw_yugioh.duckdb --into data/raw.duckdb
```

## Running dbt

```bash
cd dbt
../.venv/bin/dbt deps                          # install packages
../.venv/bin/dbt seed --profiles-dir .         # load reference CSVs
../.venv/bin/dbt run --profiles-dir .          # run all models
../.venv/bin/dbt test --profiles-dir .         # run data tests

# Partial runs
../.venv/bin/dbt run --profiles-dir . --select staging
../.venv/bin/dbt run --profiles-dir . --select int_card_editions+
```

dbt schemas in DuckDB: `main_staging`, `main_intermediate`, `main_marts`, `main_seeds`

## Running the Dashboard

```bash
cd evidence
npm install
npm run sources    # pull data from DuckDB into Evidence
npm run dev        # development server at http://localhost:3000
npm run build      # production build
```

Query tables in Evidence pages as `cards.main_marts.<table_name>`.

## Project Structure

```
crawlers/
  official/
    base.py          # OfficialCard dataclass + OfficialCrawler ABC
    zx.py            # Z/X official crawler
    yugioh.py        # Yu-Gi-Oh OCG crawler (db.yugioh-card.com)
    vanguard.py      # Cardfight!! Vanguard crawler (cf-vanguard.com)
    weiss.py         # Weiss Schwarz crawler (ws-tcg.com)
    digimon.py       # Digimon Card Game crawler (digimoncard.com)
    unionarena.py    # Union Arena TCG crawler (unionarena-tcg.com)
  shops/
    base.py          # ShopListing dataclass + ShopCrawler ABC
    yuyutei.py       # YuYuTei (yuyu-tei.jp) — Z/X, Yu-Gi-Oh
    bigweb.py        # Bigweb (bigweb.co.jp) — Z/X, Yu-Gi-Oh, Vanguard, Weiss, Digimon, UA
    cardrush.py      # Card Rush family — cardrush.jp / cardrush-vanguard.jp / cardrush-digimon.jp
  storage.py         # DuckDB write helpers

dbt/
  models/
    staging/         # stg_official_cards, stg_shop_listings
    intermediate/    # int_card_editions, int_card_base, int_shop_prices_matched
    marts/           # mart_card_price_latest/stats/history
  seeds/
    tcg_catalog.csv          # supported TCGs
    rarity_alias_map.csv     # maps shop rarity strings to canonical names

evidence/
  pages/
    index.md         # top cards by price
    zx_prices.md     # Z/X set picker with shop price comparison

data/
  raw.duckdb         # local DuckDB database (not committed)
```

## Card Numbering Schemes

Three schemes are encoded in `numbering_scheme` on every official card record:

| Scheme | Description | Match logic |
|---|---|---|
| `shared_official` | Same card number across rarities; rarity names are official (e.g. Yu-Gi-Oh) | Match on `(card_number, rarity_code)` |
| `unique_per_rarity` | Different card number per rarity; logical card grouped by `card_base_id` (e.g. Yu-Gi-Oh alt art) | Match on `card_number` alone |
| `shared_no_official` | Same card number across rarities; no official rarity name | Use `rarity_alias_map` to map shop rarity strings to canonical |

## Adding a New Crawler

**Official:** subclass `OfficialCrawler` in `crawlers/official/`, implement `crawl_sets()` and `crawl_cards()`. Set `numbering_scheme` correctly on each `OfficialCard`.

**Shop:** subclass `ShopCrawler` in `crawlers/shops/`, implement `crawl_set()` and `search_card()`. Populate `rarity_raw` with the shop's raw rarity string. Add new rarity aliases to `dbt/seeds/rarity_alias_map.csv`.

## Adding a New TCG

1. Add a row to `dbt/seeds/tcg_catalog.csv`
2. Add rarity rows to `dbt/seeds/rarity_alias_map.csv`
3. Create `crawlers/official/{tcg}.py` with the correct `numbering_scheme`
4. Re-run `dbt seed` then `dbt run`
