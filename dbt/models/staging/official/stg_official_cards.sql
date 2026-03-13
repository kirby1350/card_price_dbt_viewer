-- Normalize raw official card records into a clean staging schema.
-- One row per (tcg, card_number, rarity_code) — the finest granularity
-- from the official source.
-- Sources: DuckDB (ZX, YuGiOh) and PostgreSQL via pg attachment (Union Arena).

with source as (
    select * from {{ source('raw', 'raw_official_cards') }}
    union all
    select * from {{ source('raw_pg', 'raw_official_cards') }}
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
        coalesce(image_url, json_extract_string(extra, '$.image_url')) as image_url,
        crawled_at
    from source
    where card_number is not null
      and card_name is not null
)

select * from cleaned
