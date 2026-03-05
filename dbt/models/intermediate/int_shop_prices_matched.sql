-- int_shop_prices_matched: match shop listings to canonical card_edition_id.
--
-- Matching strategy (in order of precedence):
--   1. Exact card_number match after normalization
--   2. card_number match + rarity_alias_map lookup for rarity
--   3. Unmatched listings are kept with card_edition_id = NULL for audit

with listings as (
    select * from {{ ref('stg_shop_listings') }}
),

editions as (
    select * from {{ ref('int_card_editions') }}
),

rarity_map as (
    select * from {{ ref('rarity_alias_map') }}
),

-- Step 1: try exact match on card_number (covers unique_per_rarity and shared_official
--         when shop uses same rarity code)
exact_match as (
    select
        l.*,
        e.card_edition_id,
        'exact' as match_method
    from listings l
    inner join editions e
        on  l.tcg = e.tcg
        and l.card_number_raw = e.card_number
        and (
            -- unique_per_rarity: number already unique, no rarity match needed
            e.numbering_scheme = 'unique_per_rarity'
            -- shared schemes: also match on canonical rarity via alias map
            or upper(trim(l.rarity_raw)) = e.rarity_code
        )
),

-- Step 2: rarity alias fallback for shared schemes
alias_match as (
    select
        l.*,
        e.card_edition_id,
        'alias' as match_method
    from listings l
    -- only for rows not already matched
    left join exact_match em on l.url = em.url and em.card_edition_id is not null
    inner join rarity_map rm
        on  l.tcg = rm.tcg
        and upper(trim(l.rarity_raw)) = upper(trim(rm.shop_rarity_raw))
        and (rm.shop is null or rm.shop = l.shop)
    inner join editions e
        on  l.tcg = e.tcg
        and l.card_number_raw = e.card_number
        and rm.canonical_rarity_code = e.rarity_code
    where em.card_edition_id is null
),

combined as (
    select * from exact_match
    union all
    select * from alias_match
    union all
    -- Keep unmatched rows for audit
    select
        l.*,
        null as card_edition_id,
        'unmatched' as match_method
    from listings l
    left join exact_match em on l.url = em.url
    left join alias_match  am on l.url = am.url
    where em.card_edition_id is null and am.card_edition_id is null
)

select * from combined
