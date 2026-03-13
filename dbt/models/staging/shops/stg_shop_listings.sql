-- Normalize raw shop listings into a clean staging schema.
-- Rarity and card_number are kept as-is (_raw suffix) because they
-- require matching against official data in the intermediate layer.

with source as (
    select * from {{ source('raw', 'raw_shop_listings') }}
    union all
    select * from {{ source('raw_pg', 'raw_shop_listings') }}
),

cleaned as (
    select
        shop,
        tcg,
        upper(trim(set_code))        as set_code,
        upper(trim(card_number_raw)) as card_number_raw,
        trim(card_name_raw)          as card_name_raw,
        trim(rarity_raw)             as rarity_raw,
        upper(trim(condition))       as condition,
        price,
        upper(trim(currency))        as currency,
        quantity,
        url,
        crawled_at
    from source
    where price > 0
      and card_number_raw is not null
)

select * from cleaned
