-- Daily price snapshots per card_edition per shop.
-- Used for price trend charts in Evidence.

with matched as (
    select * from {{ ref('int_shop_prices_matched') }}
    where card_edition_id is not null
)

select
    card_edition_id,
    shop,
    condition,
    currency,
    date_trunc('day', crawled_at)   as price_date,
    min(price)                       as price_min,
    max(price)                       as price_max,
    avg(price)                       as price_avg,
    sum(quantity)                    as total_quantity
from matched
group by 1, 2, 3, 4, 5
