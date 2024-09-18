{{ config(
    materialized = 'table',
    schema = 'dev_adventure_works'
) }}

with sales_person_summary as (
    select
        sales_person_id
        , min(order_date) as first_order_date
        , extract(year from order_date) as year
        , extract(week from order_date) as week
        , date_trunc(order_date, week(monday)) as week_start_date
        , count(distinct sales_order_id) as num_orders
        , sum(order_qty) as total_products_sold
        , sum((unit_price * (1 - unit_price_discount)) * order_qty) as total_sales_value
    from {{ ref('fact_order_item') }}
    group by sales_person_id, year, week, week_start_date
)

select
    sp.sales_person_id
    , sp.first_name
    , sp.last_name
    , sps.week_start_date
    , sps.num_orders
    , sps.total_products_sold
    , sps.total_sales_value
    , st.sales_territory_name
from sales_person_summary sps
join {{ ref('dim_sales_person') }} sp
    on sps.sales_person_id = sp.sales_person_id
join {{ ref('dim_sales_territory') }} st
    on sp.sales_territory_id = st.sales_territory_id
order by sp.sales_person_id, sps.week_start_date
