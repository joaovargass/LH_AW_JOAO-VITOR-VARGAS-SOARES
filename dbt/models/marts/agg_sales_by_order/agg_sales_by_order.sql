{{ config(
    materialized = 'table',
    schema = 'dev_adventure_works'
) }}

with order_summary as (
    select
        sales_order_id
        , min(order_date) as order_date
        , max(due_date) as due_date
        , max(status_name) as status_name
        , max(sales_person_id) as sales_person_id
        , max(customer_id) as customer_id
        , max(sales_territory_id) as sales_territory_id
        , max(ship_method_name) as ship_method_name
        , max(ship_date) as ship_date
        , max(online_order_flag) as online_order_flag
        , max(store_id) as store_id
        , count(sales_order_detail_id) as num_order_items
        , sum(order_qty) as total_products_ordered
        , sum((unit_price * (1 - unit_price_discount)) * order_qty) as total_sales_value
        , avg((unit_price * (1 - unit_price_discount))) as avg_item_value
        , payment_method
        , string_agg(sales_reason_names, ', ') as sales_reasons
        , case
            when max(special_offer_id) != 1 then true
            else false
          end as has_discount
    from {{ ref('fact_order_item') }}
    group by sales_order_id, payment_method
)

select
    order_summary.sales_order_id
    , order_summary.order_date
    , order_summary.due_date
    , order_summary.status_name
    , order_summary.sales_person_id
    , order_summary.customer_id
    , order_summary.sales_territory_id
    , order_summary.ship_method_name
    , order_summary.ship_date
    , order_summary.online_order_flag
    , order_summary.store_id
    , order_summary.num_order_items
    , order_summary.total_products_ordered
    , order_summary.total_sales_value
    , order_summary.avg_item_value
    , order_summary.payment_method
    , order_summary.sales_reasons
    , order_summary.has_discount
from order_summary
order by order_summary.sales_order_id
