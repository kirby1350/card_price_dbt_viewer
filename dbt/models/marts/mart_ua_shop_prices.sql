-- Union Arena card prices across all UA shops (torecatchi, bigweb-ua, ...).
-- One row per card edition; pivot one column-pair per shop.
-- Filter by set_code (e.g. UA01BT, EX13BT) to get a specific set.

with editions as (
    select * from {{ ref('int_card_editions') }}
    where tcg = 'unionarena'
),

latest as (
    select * from {{ ref('mart_card_price_latest') }}
    where tcg = 'unionarena'
)

select
    e.card_edition_id,
    e.set_code,
    e.set_name,
    e.card_number,
    e.card_name,
    e.rarity_code,
    e.rarity_name,
    e.image_url,

    max(case when l.shop = 'torecatchi' then l.price    end) as torecatchi_price,
    max(case when l.shop = 'torecatchi' then l.quantity end) as torecatchi_stock,
    max(case when l.shop = 'torecatchi' then l.url      end) as torecatchi_url,

    max(case when l.shop = 'bigweb'     then l.price    end) as bigweb_price,
    max(case when l.shop = 'bigweb'     then l.quantity end) as bigweb_stock,
    max(case when l.shop = 'bigweb'     then l.url      end) as bigweb_url,

    max(case when l.shop = 'yuyutei'   then l.price    end) as yuyutei_price,
    max(case when l.shop = 'yuyutei'   then l.quantity end) as yuyutei_stock,
    max(case when l.shop = 'yuyutei'   then l.url      end) as yuyutei_url,

    -- cheapest available price across all shops
    least(
        max(case when l.shop = 'torecatchi' then l.price end),
        max(case when l.shop = 'bigweb'     then l.price end),
        max(case when l.shop = 'yuyutei'    then l.price end)
    ) as best_price

from editions e
left join latest l on e.card_edition_id = l.card_edition_id
group by
    e.card_edition_id,
    e.set_code,
    e.set_name,
    e.card_number,
    e.card_name,
    e.rarity_code,
    e.rarity_name,
    e.image_url
order by e.set_code, e.card_number, e.rarity_code
