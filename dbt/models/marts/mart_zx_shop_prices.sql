-- Pre-pivoted ZX card prices by shop (yuyutei + bigweb).
-- One row per card edition; Evidence pages filter by set_code for fast lookup.

with editions as (
    select * from {{ ref('int_card_editions') }}
    where tcg = 'zx'
),

latest as (
    select * from {{ ref('mart_card_price_latest') }}
    where tcg = 'zx'
)

select
    e.card_edition_id,
    e.set_code,
    e.set_name,
    e.card_number,
    e.card_name,
    e.rarity_code,
    e.rarity_name,

    max(case when l.shop = 'yuyutei' then l.price    end) as yuyutei_price,
    max(case when l.shop = 'yuyutei' then l.quantity end) as yuyutei_stock,
    max(case when l.shop = 'yuyutei' then l.url      end) as yuyutei_url,

    max(case when l.shop = 'bigweb'  then l.price    end) as bigweb_price,
    max(case when l.shop = 'bigweb'  then l.quantity end) as bigweb_stock,
    max(case when l.shop = 'bigweb'  then l.url      end) as bigweb_url,

    case
        when max(case when l.shop = 'yuyutei' then l.price end) is not null
         and max(case when l.shop = 'bigweb'  then l.price end) is not null
        then max(case when l.shop = 'yuyutei' then l.price end)
           - max(case when l.shop = 'bigweb'  then l.price end)
    end as price_diff

from editions e
left join latest l on e.card_edition_id = l.card_edition_id
group by
    e.card_edition_id,
    e.set_code,
    e.set_name,
    e.card_number,
    e.card_name,
    e.rarity_code,
    e.rarity_name
order by e.set_code, e.card_number
