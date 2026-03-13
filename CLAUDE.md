# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A card price tracking and visualization system for TCG cards. Three major components:
1. **Crawlers** — scrape official card lists and shop prices
2. **DBT** — transform raw data into normalized, analytics-ready models
3. **Evidence** — display price statistics as a dashboard

**Supported TCGs:** Yu-Gi-Oh, Z/X -Zillions of enemy X-, Cardfight!! Vanguard, Weiss Schwarz, Digimon Card Game, Union Arena
**Stack:** Python 3.12, DuckDB, dbt-duckdb, Evidence.dev

## Setup

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

Run dbt (from `dbt/` directory):
```bash
cd dbt
dbt deps
dbt seed          # load reference CSVs (tcg_catalog, rarity_alias_map)
dbt run           # run all models
dbt test          # run data tests
dbt run --select staging   # run only staging layer
dbt run --select int_card_editions+  # run a model and its dependents
```

Run Evidence (from `evidence/` directory):
```bash
cd evidence
npm install
npm run dev       # development server
npm run build     # production build
```

## Architecture

### Data Flow

```
Official sites / Shops
        ↓ crawlers/
DuckDB raw tables (raw_official_cards, raw_shop_listings)
        ↓ dbt staging/
Cleaned, normalized views per source
        ↓ dbt intermediate/
int_card_editions   — canonical tradeable unit (tcg + card_number + rarity_code)
int_card_base       — logical card identity (groups editions across rarities)
int_shop_prices_matched — shop listings matched to card_edition_id
        ↓ dbt marts/
mart_card_price_latest   — most recent price per edition per shop
mart_card_price_stats    — min/max/avg/median aggregates
mart_card_price_history  — daily snapshots for trend charts
        ↓ Evidence.dev
Dashboard pages querying mart_ tables
```

### Card Numbering Schemes

Three schemes are encoded in `numbering_scheme` on every official card record:

| Scheme | Description | Matching logic |
|--------|-------------|----------------|
| `shared_official` | Same card_number across rarities; rarity names are official (e.g. Z/X) | Match on `(card_number, rarity_code)` |
| `unique_per_rarity` | Different card_number per rarity; same logical card grouped by `card_base_id` (e.g. Yu-Gi-Oh) | Match on `card_number` alone |
| `shared_no_official` | Same card_number across rarities; no official rarity name | Use `rarity_alias_map` seed to map shop rarity strings to canonical |

The official card list is always the source of truth for canonical rarity names. Shop rarity strings are mapped via `dbt/seeds/rarity_alias_map.csv`.

### Key Files

| File | Purpose |
|------|---------|
| `crawlers/official/base.py` | `OfficialCard` dataclass + `OfficialCrawler` ABC |
| `crawlers/shops/base.py` | `ShopListing` dataclass + `ShopCrawler` ABC |
| `crawlers/storage.py` | DuckDB write helpers; DB at `data/raw.duckdb` |
| `dbt/models/intermediate/int_card_editions.sql` | Core identity model |
| `dbt/models/intermediate/int_shop_prices_matched.sql` | Rarity matching logic |
| `dbt/seeds/rarity_alias_map.csv` | Maps shop rarity strings → canonical per TCG |

## Running Crawlers

```bash
# Official card lists
python main.py crawl zx-official
python main.py crawl yugioh-official
python main.py crawl vanguard-official
python main.py crawl weiss-official
python main.py crawl digimon-official
python main.py crawl unionarena-official

# Shop prices — YuYuTei
python main.py crawl yuyutei-zx
python main.py crawl yuyutei-ygo

# Shop prices — Bigweb (game_id/game_code in bigweb.py docstring)
python main.py crawl bigweb-zx
python main.py crawl bigweb-yugioh
python main.py crawl bigweb-vanguard
python main.py crawl bigweb-weiss
python main.py crawl bigweb-digimon
python main.py crawl bigweb-unionarena

# Shop prices — Card Rush
python main.py crawl cardrush-ygo       # cardrush.jp
python main.py crawl cardrush-vanguard  # cardrush-vanguard.jp
python main.py crawl cardrush-digimon   # cardrush-digimon.jp

# Shop prices — Masters Square
python main.py crawl mastersquare-ua    # masters-square.com (Union Arena)

# Shop prices — Hobby Station
python main.py crawl hobbystation-ua    # hobbystation-single.jp (Union Arena)

# Common options
python main.py crawl <target> --set <code>   # single set/product-group (testing)
python main.py crawl <target> --delay 2.0    # seconds between requests (default: 1.0)
python main.py crawl <target> --debug        # enable DEBUG logging
python main.py crawl <target> --db data/raw_foo.duckdb  # write to separate file
```

Raw data lands in `data/raw.duckdb`. To run crawlers in parallel without write conflicts, use `--db` to write separate files then merge:

```bash
python main.py crawl bigweb-yugioh --db data/raw_bigweb_yugioh.duckdb
python main.py crawl cardrush-ygo  --db data/raw_cardrush_ygo.duckdb
# ... other crawlers in parallel ...
python main.py merge   # consolidates all raw_*.duckdb into data/raw.duckdb
```

### Adding a New Crawler

**Official crawler:** subclass `OfficialCrawler` in `crawlers/official/`, implement `crawl_sets()` and `crawl_cards()`. Set `numbering_scheme` correctly on each `OfficialCard`.

**Shop crawler:** subclass `ShopCrawler` in `crawlers/shops/`, implement `crawl_set()` and `search_card()`. Populate `rarity_raw` with the shop's raw rarity string (mapping happens in dbt). Add new rarity aliases to `dbt/seeds/rarity_alias_map.csv`.

### Adding a New TCG

1. Add row to `dbt/seeds/tcg_catalog.csv`
2. Add rarity rows to `dbt/seeds/rarity_alias_map.csv`
3. Create `crawlers/official/{tcg}.py` with the correct `numbering_scheme`
4. If the TCG uses `shared_no_official`, confirm that official card list entries define the canonical editions — the rarity_alias_map handles shop→canonical mapping
