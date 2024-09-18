{{ config(
    materialized = 'table',
    schema = 'dev_adventure_works'
) }}

with customer_sales_data as (
    select
        sales_order_id
        , customer_id
        , product_id
        , order_qty
        , unit_price
        , unit_price_discount
        , order_date
        , (unit_price * (1 - unit_price_discount)) * order_qty as total_value
    from {{ ref('fact_order_item') }}
)

, aggregated_customer_sales as (
    select
        customer_id
        , count(distinct sales_order_id) as num_orders
        , sum(order_qty) as total_products
        , avg(order_qty) as avg_products_per_order
        , sum(total_value) as total_sales_value
        , avg(total_value) as avg_order_value
        , max(order_date) as last_order_date
    from customer_sales_data
    group by customer_id
)

, top_customer_products as (
    select
        customer_id
        , product_id
        , count(sales_order_id) as num_orders_with_product
        , sum(order_qty) as total_qty_with_product
        , rank() over (partition by customer_id order by sum(order_qty) desc) as rank_product
    from customer_sales_data
    group by customer_id, product_id
)

select
    aggregated_customer_sales.customer_id
    , aggregated_customer_sales.num_orders
    , aggregated_customer_sales.total_products
    , aggregated_customer_sales.avg_products_per_order
    , aggregated_customer_sales.total_sales_value
    , aggregated_customer_sales.avg_order_value
    , aggregated_customer_sales.last_order_date
    , top_customer_products.product_id as most_purchased_product
    , top_customer_products.total_qty_with_product
    , top_customer_products.num_orders_with_product
from aggregated_customer_sales
left join top_customer_products
    on aggregated_customer_sales.customer_id = top_customer_products.customer_id
    and top_customer_products.rank_product = 1
order by aggregated_customer_sales.customer_id
