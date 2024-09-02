
{{ config(
    materialized='table',
    schema='dev_adventure_works'
) }}

with product_sales as (
    select
        p.product_id
        , p.product_line
        , p.class
        , p.style
        , p.color
        , p.size
        , p.weight
        , p.standard_cost
        , p.list_price
        , p.finished_goods_flag
        , p.product_name
        , p.product_category_name
        , p.product_subcategory_name
        , p.product_model_name
        , st.sales_territory_id
        , st.sales_territory_name
        , st.sales_territory_group
        , st.country_region_id
        , st.country_region_name
        , so.special_offer_id
        , so.offer_description
        , so.discount_pct
        , so.offer_type
        , so.offer_category
        , f.online_order_flag
        , extract(year from f.order_date) as year
        , extract(week from f.order_date) as week_of_year
        , round(sum(f.order_qty), 2) as sales_quantity
        , round(sum(f.unit_price * f.order_qty), 2) as sales_value
        , round(sum(f.unit_price_discount * f.order_qty), 2) as sales_with_discount_value
        , count(f.sales_order_id) as number_of_orders
    from
        {{ ref('fact_order_item') }} f
    join
        {{ ref('dim_product') }} p on f.product_id = p.product_id
    join
        {{ ref('dim_sales_territory') }} st on f.sales_territory_id = st.sales_territory_id
    join
        {{ ref('dim_special_offer') }} so on f.special_offer_id = so.special_offer_id
    group by
        p.product_id
        , p.product_line
        , p.class
        , p.style
        , p.color
        , p.size
        , p.weight
        , p.standard_cost
        , p.list_price
        , p.finished_goods_flag
        , p.product_name
        , p.product_category_name
        , p.product_subcategory_name
        , p.product_model_name
        , st.sales_territory_id
        , st.sales_territory_name
        , st.sales_territory_group
        , st.country_region_id
        , st.country_region_name
        , so.special_offer_id
        , so.offer_description
        , so.discount_pct
        , so.offer_type
        , so.offer_category
        , f.online_order_flag
        , year
        , week_of_year
)

select
    product_id
    , product_line
    , class
    , style
    , color
    , size
    , weight
    , standard_cost
    , list_price
    , finished_goods_flag
    , product_name
    , product_category_name
    , product_subcategory_name
    , product_model_name
    , sales_territory_id
    , sales_territory_name
    , sales_territory_group
    , country_region_id
    , country_region_name
    , special_offer_id
    , offer_description
    , discount_pct
    , offer_type
    , offer_category
    , online_order_flag
    , week_of_year
    , year
    , sales_quantity
    , sales_value
    , sales_with_discount_value
    , safe_divide(sales_value, number_of_orders) as average_ticket
    , number_of_orders
from
    product_sales
