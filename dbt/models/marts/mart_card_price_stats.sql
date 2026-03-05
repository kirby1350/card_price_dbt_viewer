-- Aggregated price statistics per card_edition across all shops.
-- Joined with edition metadata for display.

with latest as (
    select * from {{ ref('mart_card_price_latest') }}
),

editions as (
    select * from {{ ref('int_card_editions') }}
)

select
    l.card_edition_id,
    e.tcg,
    e.set_code,
    e.set_name,
    e.card_number,
    e.card_name,
    e.rarity_code,
    e.rarity_name,
    l.condition,
    l.currency,
    count(distinct l.shop)   as shop_count,
    count(*)                 as listing_count,
    min(l.price)             as price_min,
    max(l.price)             as price_max,
    avg(l.price)             as price_avg,
    median(l.price)          as price_median,
    sum(l.quantity)          as total_quantity,
    max(l.crawled_at)        as last_updated
from latest l
inner join editions e using (card_edition_id)
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
