-- Normalize raw official card records into a clean staging schema.
-- One row per (tcg, card_number, rarity_code) — the finest granularity
-- from the official source.

with source as (
    select * from {{ source('raw', 'raw_official_cards') }}
),

cleaned as (
    select
        tcg,
        upper(trim(set_code))    as set_code,
        trim(set_name)           as set_name,
        upper(trim(card_number)) as card_number,
        trim(card_name)          as card_name,
        upper(trim(rarity_code)) as rarity_code,
        trim(rarity_name)        as rarity_name,
        numbering_scheme,
        card_base_id,
        extra,
        crawled_at
    from source
    where card_number is not null
      and card_name is not null
)

select * from cleaned
