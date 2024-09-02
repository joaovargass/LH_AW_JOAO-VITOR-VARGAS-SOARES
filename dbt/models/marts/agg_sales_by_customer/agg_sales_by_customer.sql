{{ config(
    materialized='table',
    schema='dev_adventure_works'
) }}

with customer_sales_data as (
    select
        c.customer_id
        , c.first_name
        , c.last_name
        , c.email_promotion
        , c.country_region_id
        , c.country_region_name
        , st.sales_territory_id
        , st.sales_territory_name
        , st.sales_territory_group
        , f.store_id
        , date_trunc(f.order_date, week(monday)) as start_of_week
        , round(sum(f.unit_price * f.order_qty), 2) as total_sales_value
        , count(distinct f.sales_order_id) as total_orders
        , max(f.order_date) as last_order_date
    from
        {{ ref('fact_order_item') }} f
    join
        {{ ref('dim_customer') }} c on f.customer_id = c.customer_id
    join
        {{ ref('dim_sales_territory') }} st on c.sales_territory_id = st.sales_territory_id
    group by
        c.customer_id
        , c.first_name
        , c.last_name
        , c.email_promotion
        , c.country_region_id
        , c.country_region_name
        , st.sales_territory_id
        , st.sales_territory_name
        , st.sales_territory_group
        , f.store_id
        , start_of_week
)
select *
from
    customer_sales_data
