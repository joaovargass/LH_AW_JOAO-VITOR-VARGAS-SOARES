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
        f.store_id
        , date_trunc(f.order_date, week(monday)) as start_of_week
        , round(sum(f.unit_price * f.order_qty), 2) as total_sales_value
        , round(sum((f.unit_price - f.unit_price_discount) * f.order_qty), 2) as total_sales_value_with_discount
        , sum(f.order_qty) as total_quantity
        , round(avg(f.item_tax_rate), 4) as avg_item_tax_rate
    from
        {{ ref('fact_order_item') }} f
    group by
        f.store_id
        , date_trunc(f.order_date, week(monday))
)

select
    o.store_id
    , s.store_name
    , s.state_province_name
    , s.city_district_name
    , s.country_region_name
    , o.start_of_week
    , o.total_sales_value
    , o.total_sales_value_with_discount
    , o.total_quantity
    , o.avg_item_tax_rate
from
    order_aggregation o
left join
    {{ ref('dim_store') }} s on o.store_id = s.store_id
where
    o.store_id is not null
order by
    o.store_id, start_of_week
