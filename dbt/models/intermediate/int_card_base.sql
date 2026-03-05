-- int_card_base: one row per logical card.
-- For shared_official and shared_no_official schemes, this is the card_number.
-- For unique_per_rarity (Yu-Gi-Oh), this groups all rarities of the same card.

with editions as (
    select * from {{ ref('int_card_editions') }}
)

select
    tcg,
    card_base_id,
    -- Use the most common card_name for this base (all rarities share the same name)
    max(card_name) as card_name,
    -- Collect all set memberships for this base card
    array_agg(distinct set_code) as set_codes,
    array_agg(distinct card_number) as card_numbers
from editions
group by tcg, card_base_id
