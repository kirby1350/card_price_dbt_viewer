---
title: Z/X Card Prices
---

# Z/X Card Prices

Compare prices across YuYuTei and Bigweb for any Z/X set.

```sql set_list
select
    set_code,
    set_name,
    set_code || ' - ' || set_name as set_label,
    count(*) as card_count
from main_marts.mart_zx_shop_prices
group by set_code, set_name
order by set_code
```

<Dropdown
    data={set_list}
    name=selected_set
    value=set_code
    label=set_label
    title="Select a Set"
/>

```sql set_info
select
    set_name,
    count(*) as editions,
    count(case when yuyutei_price is not null or bigweb_price is not null then 1 end) as priced_editions,
    min(least(yuyutei_price, bigweb_price)) as lowest_price,
    max(greatest(yuyutei_price, bigweb_price)) as highest_price
from main_marts.mart_zx_shop_prices
where set_code = '${inputs.selected_set.value}'
group by set_name
```

<BigValue data={set_info} value=set_name title="Set" />
<BigValue data={set_info} value=editions title="Total Editions" />
<BigValue data={set_info} value=priced_editions title="With Price Data" />
<BigValue data={set_info} value=lowest_price title="Lowest Price (¥)" />
<BigValue data={set_info} value=highest_price title="Highest Price (¥)" />

---

## Card Price Comparison

```sql card_prices
select
    card_number,
    card_name,
    rarity_code,
    yuyutei_price,
    yuyutei_stock,
    yuyutei_url,
    bigweb_price,
    bigweb_stock,
    bigweb_url,
    price_diff
from main_marts.mart_zx_shop_prices
where set_code = '${inputs.selected_set.value}'
order by card_number
```

<DataTable
    data={card_prices}
    rows=200
    search=true
>
    <Column id=card_number title="Card #" />
    <Column id=card_name title="Name" />
    <Column id=rarity_code title="Rarity" />
    <Column id=yuyutei_price title="YuYuTei (¥)" fmt=num0 />
    <Column id=yuyutei_stock title="YYT Stock" />
    <Column id=yuyutei_url title="YYT" contentType=link linkLabel="→" />
    <Column id=bigweb_price title="Bigweb (¥)" fmt=num0 />
    <Column id=bigweb_stock title="BW Stock" />
    <Column id=bigweb_url title="BW" contentType=link linkLabel="→" />
    <Column id=price_diff title="Diff (¥)" fmt=num0 />
</DataTable>

---

## Price Summary by Rarity

```sql price_dist
select
    rarity_code,
    count(*) as card_count,
    round(avg(yuyutei_price), 0) as yuyutei_avg,
    round(avg(bigweb_price), 0)  as bigweb_avg,
    min(yuyutei_price) as yuyutei_min,
    max(yuyutei_price) as yuyutei_max,
    min(bigweb_price)  as bigweb_min,
    max(bigweb_price)  as bigweb_max
from main_marts.mart_zx_shop_prices
where set_code = '${inputs.selected_set.value}'
group by rarity_code
order by yuyutei_avg desc nulls last
```

<DataTable data={price_dist}>
    <Column id=rarity_code title="Rarity" />
    <Column id=card_count title="Cards" />
    <Column id=yuyutei_avg title="YYT Avg (¥)" fmt=num0 />
    <Column id=bigweb_avg title="BW Avg (¥)" fmt=num0 />
    <Column id=yuyutei_min title="YYT Min (¥)" fmt=num0 />
    <Column id=yuyutei_max title="YYT Max (¥)" fmt=num0 />
    <Column id=bigweb_min title="BW Min (¥)" fmt=num0 />
    <Column id=bigweb_max title="BW Max (¥)" fmt=num0 />
</DataTable>
