---
title: Card Price Dashboard
---

# Card Price Dashboard

```sql card_count
select count(distinct card_edition_id) as editions,
       count(distinct tcg) as tcgs
from main_marts.mart_card_price_stats
```

**{card_count[0].editions}** card editions tracked across **{card_count[0].tcgs}** TCGs.

→ [Z/X Set Price Comparison](/zx_prices)

## Top Cards by Price

```sql top_cards
select card_name, rarity_name, set_name, tcg,
       price_min, price_max, price_avg, shop_count
from main_marts.mart_card_price_stats
where condition = 'NM'
order by price_avg desc
limit 20
```

<DataTable data={top_cards} />
