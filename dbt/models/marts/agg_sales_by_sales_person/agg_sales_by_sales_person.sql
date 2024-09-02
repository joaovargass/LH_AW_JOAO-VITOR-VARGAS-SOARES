{{ config(
    materialized='table',
    schema='dev_adventure_works'
) }}

with sales_reasons as (
    select
        f.sales_order_id,
        string_agg(distinct trim(word), ', ') as sales_reason_names
    from
        {{ ref('fact_order_item') }} f,
        unnest(split(f.sales_reason_names, ', ')) as word
    group by
        f.sales_order_id
),

special_offers as (
    select
        f.sales_order_id,
        string_agg(distinct cast(f.special_offer_id as string), ', ') as special_offer_ids,
        string_agg(distinct so.offer_description, ', ') as offer_descriptions,
        string_agg(distinct so.offer_type, ', ') as offer_types,
        string_agg(distinct so.offer_category, ', ') as offer_categories
    from
        {{ ref('fact_order_item') }} f
    left join
        {{ ref('dim_special_offer') }} so on f.special_offer_id = so.special_offer_id
    group by
        f.sales_order_id
),

order_aggregation as (
    select
        f.sales_person_id
        , extract(year from f.order_date) as year
        , extract(quarter from f.order_date) as quarter
        , round(sum(f.unit_price * f.order_qty), 2) as total_sales_value
        , round(sum((f.unit_price - f.unit_price_discount) * f.order_qty), 2) as total_sales_value_with_discount
        , sum(f.order_qty) as total_quantity
        , round(avg(f.item_tax_rate), 4) as avg_item_tax_rate
        , round(sum(coalesce(f.unit_freight_price, 0)), 2) as total_freight
    from
        {{ ref('fact_order_item') }} f
    group by
        f.sales_person_id
        , extract(year from f.order_date)
        , extract(quarter from f.order_date)
)

select
    o.sales_person_id
    , sp.first_name
    , sp.last_name
    , sp.sales_territory_id
    , t.sales_territory_name
    , t.sales_territory_group
    , t.country_region_name
    , o.year
    , o.quarter
    , o.total_sales_value
    , o.total_sales_value_with_discount
    , o.total_quantity
    , o.avg_item_tax_rate
    , o.total_freight
from
    order_aggregation o
left join
    {{ ref('dim_sales_person') }} sp on o.sales_person_id = sp.sales_person_id
left join
    {{ ref('dim_sales_territory') }} t on sp.sales_territory_id = t.sales_territory_id
where
    o.sales_person_id is not null
order by
    o.sales_person_id, year, quarter
