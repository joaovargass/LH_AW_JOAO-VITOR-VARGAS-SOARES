{{ config(
    materialized = 'table',
    schema = 'dev_adventure_works'
) }}

with product_sales_summary as (
    select
        product_id
        , date_trunc(order_date, week(monday)) as week_start_date
        , count(distinct sales_order_id) as num_orders
        , sum(order_qty) as total_products_sold
        , sum((unit_price * (1 - unit_price_discount)) * order_qty) as total_sales_value
        , sum(case when special_offer_id != 1 then order_qty else 0 end) as products_with_discount
        , sum(case when special_offer_id != 1 then (unit_price * unit_price_discount) * order_qty else 0 end) as total_discount_value
    from {{ ref('fact_order_item') }}
    where product_id is not null
    group by product_id, week_start_date
)

select
    pss.product_id
    , p.product_name
    , p.product_category_name
    , p.product_subcategory_name
    , case
        when p.product_line = 'R' then 'Road'
        when p.product_line = 'M' then 'Mountain'
        when p.product_line = 'T' then 'Touring'
        when p.product_line = 'S' then 'Standard'
        else p.product_line
      end as product_line_description
    , p.product_model_name
    , case
        when p.class = 'H' then 'High'
        when p.class = 'M' then 'Medium'
        when p.class = 'L' then 'Low'
        else p.class
      end as class_description
    , p.color
    , case
        when p.style = 'W' then 'Womens'
        when p.style = 'M' then 'Mens'
        when p.style = 'U' then 'Universal'
        else p.style
      end as style_description
    , p.sell_start_date
    , p.sell_end_date
    , pss.week_start_date
    , pss.num_orders
    , pss.total_products_sold
    , pss.total_sales_value
    , pss.products_with_discount
    , pss.total_discount_value
from product_sales_summary pss
join {{ ref('dim_product') }} p
    on pss.product_id = p.product_id
order by pss.product_id, pss.week_start_date
