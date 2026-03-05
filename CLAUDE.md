# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A card price tracking and visualization system for TCG cards. Three major components:
1. **Crawlers** — scrape official card lists and shop prices
2. **DBT** — transform raw data into normalized, analytics-ready models
3. **Evidence** — display price statistics as a dashboard

**Supported TCGs:** Yu-Gi-Oh, Z/X -Zillions of enemy X-
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
# Full crawl of all Z/X sets (skips already-crawled sets)
python main.py crawl zx-official

# Single set (useful for testing)
python main.py crawl zx-official --set B01

# With debug logging
python main.py crawl zx-official --set B01 --debug

# Adjust request delay (seconds between HTTP requests)
python main.py crawl zx-official --delay 2.0
```

Raw data lands in `data/raw.duckdb`. The full Z/X crawl covers ~300 sets; at 1.5s delay it takes several hours.

### Adding a New Crawler

**Official crawler:** subclass `OfficialCrawler` in `crawlers/official/`, implement `crawl_sets()` and `crawl_cards()`. Set `numbering_scheme` correctly on each `OfficialCard`.

**Shop crawler:** subclass `ShopCrawler` in `crawlers/shops/`, implement `crawl_set()` and `search_card()`. Populate `rarity_raw` with the shop's raw rarity string (mapping happens in dbt). Add new rarity aliases to `dbt/seeds/rarity_alias_map.csv`.

### Adding a New TCG

1. Add row to `dbt/seeds/tcg_catalog.csv`
2. Add rarity rows to `dbt/seeds/rarity_alias_map.csv`
3. Create `crawlers/official/{tcg}.py` with the correct `numbering_scheme`
4. If the TCG uses `shared_no_official`, confirm that official card list entries define the canonical editions — the rarity_alias_map handles shop→canonical mapping
