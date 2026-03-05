-- int_card_editions: the atomic tradeable unit.
-- One row per (tcg, card_number, rarity_code) as defined by the official source.
--
-- Handles all three numbering schemes:
--   shared_official    — card_number shared, rarity is official → (card_number + rarity_code) is unique
--   unique_per_rarity  — card_number already unique per rarity → card_number alone is unique
--   shared_no_official — card_number shared, no official rarity name → use rarity_alias_map for
--                        canonical rarity; official card list entries define the editions

with official as (
    select * from {{ ref('stg_official_cards') }}
),

rarity_map as (
    select * from {{ ref('rarity_alias_map') }}
),

editions as (
    select
        -- Surrogate key: stable identifier for this edition
        {{ dbt_utils.generate_surrogate_key(['tcg', 'card_number', 'rarity_code']) }} as card_edition_id,

        tcg,
        set_code,
        set_name,
        card_number,
        card_name,
        rarity_code,
        rarity_name,
        numbering_scheme,

        -- card_base_id: for unique_per_rarity, populated by crawler from official source.
        -- For other schemes, the card_number itself identifies the base card.
        coalesce(
            card_base_id,
            card_number
        ) as card_base_id,

        crawled_at
    from official
)

select * from editions
