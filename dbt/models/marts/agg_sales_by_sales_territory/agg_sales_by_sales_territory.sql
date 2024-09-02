{{ config(
    materialized='table',
    schema='dev_adventure_works'
) }}

with sales_territory_data as (
    select
        st.sales_territory_id
        , st.sales_territory_name
        , st.sales_territory_group
        , st.country_region_id
        , st.country_region_name
        , so.special_offer_id
        , so.offer_description
        , so.discount_pct
        , so.offer_type
        , so.offer_category
        , f.sales_person_id
        , extract(year from f.order_date) as year
        , extract(week from f.order_date) as week_of_year
        , sum(f.order_qty) as total_quantity
        , round(sum(f.unit_price * f.order_qty), 2) as total_sales_value
        , round(sum(f.unit_price_discount * f.order_qty), 2) as total_discount_value
        , count(f.sales_order_id) as total_orders
    from
        {{ ref('fact_order_item') }} f
    join
        {{ ref('dim_sales_territory') }} st on f.sales_territory_id = st.sales_territory_id
    join
        {{ ref('dim_sales_person') }} sp on f.sales_person_id = sp.sales_person_id
    left join
        {{ ref('dim_special_offer') }} so on f.special_offer_id = so.special_offer_id
    group by
        st.sales_territory_id
        , st.sales_territory_name
        , st.sales_territory_group
        , st.country_region_id
        , st.country_region_name
        , so.special_offer_id
        , so.offer_description
        , so.discount_pct
        , so.offer_type
        , so.offer_category
        , f.sales_person_id
        , year
        , week_of_year
)

select *
from
    sales_territory_data
