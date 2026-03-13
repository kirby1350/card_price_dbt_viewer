# TODO

## Crawlers

### New Shops
- [ ] Add YuYuTei Union Arena crawler (`yuyutei-ua` exists but only covers a few sets)
- [ ] Add Card Rush Union Arena crawler (`cardrush-ua`)
- [ ] Add Bigweb crawlers for remaining TCGs (Weiss Schwarz, Vanguard, Digimon already supported)

### Improvements
- [ ] Hobby Station: enrich `rarity_raw` with base rarity (SR/R/U/C) by looking up official card list post-crawl, instead of relying on empty/star-only matching in the web API
- [ ] Masters Square: filter category discovery to exclude non-UA navigation links (currently ~60 empty categories are fetched and skipped)
- [ ] Add retry/resume support for PostgreSQL connection drops during long crawls
- [ ] Deduplicate listings when the same card appears in multiple categories (Masters Square)

## Web Viewer
- [ ] Add price history chart (currently only shows latest prices)
- [ ] Add search functionality (search by card name across sets)
- [ ] Show card images in listing view
- [ ] Add stock quantity display (currently available but not shown)
- [ ] Mobile-responsive layout improvements

## DBT
- [ ] Add data tests for new shops (hobbystation, mastersquare)
- [ ] Add mart model for price comparison across shops (cheapest source per card)
- [ ] Add mart model for price trends (weekly/monthly aggregates)

## Infrastructure
- [ ] Add scheduled crawl automation (cron / GitHub Actions)
- [ ] Add health check endpoint to web API
- [ ] Add logging/monitoring for crawl failures
