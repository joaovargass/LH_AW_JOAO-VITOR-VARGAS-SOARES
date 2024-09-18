{{ config(
    materialized = 'table',
    schema = 'dev_adventure_works'
) }}

with store_sales_summary as (
    select
        store_id
        , date_trunc(order_date, week(monday)) as week_start_date
        , count(distinct sales_order_id) as num_orders
        , sum(order_qty) as total_products_sold
        , sum((unit_price * (1 - unit_price_discount)) * order_qty) as total_sales_value
        , avg((unit_price * (1 - unit_price_discount)) * order_qty) as avg_sales_value_per_order
        , avg(order_qty) as avg_products_per_order
    from {{ ref('fact_order_item') }}
    where store_id is not null
    group by store_id, week_start_date
)

select
    ss.store_id
    , s.store_name
    , ss.week_start_date
    , ss.num_orders
    , ss.total_products_sold
    , ss.total_sales_value
    , ss.avg_sales_value_per_order
    , ss.avg_products_per_order
    , s.state_province_name
    , s.country_region_name
from store_sales_summary ss
join {{ ref('dim_store') }} s
    on ss.store_id = s.store_id
order by ss.store_id, ss.week_start_date
