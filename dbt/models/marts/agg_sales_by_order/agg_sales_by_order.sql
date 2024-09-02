{{ config(
    materialized='table',
    schema='dev_adventure_works'
) }}

with sales_reasons_cte as (
    select
        f.sales_order_id,
        string_agg(distinct trim(word), ', ') as sales_reason_names
    from
        {{ ref('fact_order_item') }} f,
        unnest(split(f.sales_reason_names, ', ')) as word
    group by
        f.sales_order_id
),

special_offers_cte as (
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

order_aggregation_cte as (
    select
        f.sales_order_id
        , f.customer_id
        , f.sales_person_id
        , f.sales_territory_id
        , r.sales_reason_names
        , round(sum(coalesce(f.unit_freight_price, 0)), 2) as freight_price
        , round(avg(f.item_tax_rate), 4) as avg_item_tax_rate
        , f.store_id
        , f.order_date
        , f.ship_date
        , round(sum(f.unit_price * f.order_qty), 2) as order_value
        , round(sum((f.unit_price - f.unit_price_discount) * f.order_qty), 2) as discount_value
        , sum(f.order_qty) as total_qty
        , f.currency_rate_id
        , f.from_currency_code
        , f.to_currency_code
        , f.average_rate
        , f.credit_card_type
        , round(sum((f.unit_price - f.unit_price_discount) * f.order_qty + coalesce(f.unit_freight_price, 0)), 2) as value_with_freight
        , f.due_date
        , so.special_offer_ids
        , so.offer_descriptions
        , so.offer_types
        , so.offer_categories
        , f.status
        , f.online_order_flag
        , f.ship_method_id
        , f.ship_method_name
    from
        {{ ref('fact_order_item') }} f
    left join
        sales_reasons_cte r on f.sales_order_id = r.sales_order_id
    left join
        special_offers_cte so on f.sales_order_id = so.sales_order_id
    group by
        f.sales_order_id
        , f.customer_id
        , f.sales_person_id
        , f.sales_territory_id
        , r.sales_reason_names
        , f.store_id
        , f.order_date
        , f.ship_date
        , f.currency_rate_id
        , f.from_currency_code
        , f.to_currency_code
        , f.average_rate
        , f.credit_card_type
        , f.due_date
        , so.special_offer_ids
        , so.offer_descriptions
        , so.offer_types
        , so.offer_categories
        , f.status
        , f.online_order_flag
        , f.ship_method_id
        , f.ship_method_name
)

select *
from
    order_aggregation_cte
order by
    sales_order_id
