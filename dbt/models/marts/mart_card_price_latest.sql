-- Latest price per card_edition per shop (most recent crawl only).

with matched as (
    select * from {{ ref('int_shop_prices_matched') }}
    where card_edition_id is not null
),

ranked as (
    select
        *,
        row_number() over (
            partition by card_edition_id, shop, condition
            order by crawled_at desc
        ) as rn
    from matched
)

select
    card_edition_id,
    shop,
    tcg,
    card_number_raw,
    card_name_raw,
    rarity_raw,
    condition,
    price,
    currency,
    quantity,
    url,
    crawled_at,
    match_method
from ranked
where rn = 1
