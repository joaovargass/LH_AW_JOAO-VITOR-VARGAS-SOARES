{{ config(
    materialized = 'table',
    schema = 'dev_adventure_works'
) }}

with sales_territory_summary as (
    select
        sales_territory_id
        , date_trunc(order_date, week(monday)) as week_start_date
        , count(distinct sales_order_id) as num_orders
        , sum(order_qty) as total_products_sold
        , sum((unit_price * (1 - unit_price_discount)) * order_qty) as total_sales_value
        , count(distinct customer_id) as num_customers
    from {{ ref('fact_order_item') }}
    where sales_territory_id is not null
    group by sales_territory_id, week_start_date
)

select
    sts.sales_territory_id
    , st.sales_territory_name
    , sts.week_start_date
    , sts.num_orders
    , sts.total_products_sold
    , sts.total_sales_value
    , sts.num_customers
from sales_territory_summary sts
join {{ ref('dim_sales_territory') }} st
    on sts.sales_territory_id = st.sales_territory_id
order by sts.sales_territory_id, sts.week_start_date
