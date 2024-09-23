{{ config(
    materialized = 'table',
    schema = 'dev_adventure_works'
) }}

WITH first_purchase_check AS (
    SELECT
        customer_id
        , product_id
        , MIN(order_date) AS first_order_date
    FROM
        dev_adventure_works.fact_order_item
    GROUP BY
        customer_id
        , product_id
)

, weekly_sales AS (
    SELECT
        p.product_id
        , p.product_name
        , p.product_category_name
        , p.product_subcategory_name
        , p.product_line
        , p.class
        , p.style
        , p.make_flag
        , p.safety_stock_level
        , p.reorder_point
        , p.days_to_manufacture
        , p.sell_start_date
        , p.sell_end_date
        , p.finished_goods_flag
        , COALESCE(st.sales_territory_name, 'Unknown') AS sales_territory_name
        , COALESCE(c.city_district_name, 'Unknown') AS city_district_name
        , COALESCE(c.state_province_name, 'Unknown') AS state_province_name
        , COALESCE(s.store_name, 'Unknown') AS store_name
        , s.store_id
        , DATE_TRUNC(foi.order_date, WEEK(MONDAY)) AS week_start_date
        , SUM(foi.order_qty) AS total_products_sold
        , foi.unit_price
        , foi.unit_price_discount
        , avg(foi.avg_unit_freight) as avg_unit_freight
        , foi.online_order_flag
        , foi.ship_method_name
        , foi.sales_person_id
        , foi.sales_reason_names
        , foi.sales_reason_types
        , dimso.discount_pct
        , CASE
            WHEN fp.first_order_date = MIN(foi.order_date) THEN TRUE
            ELSE FALSE
          END AS is_first_purchase
        , CASE
            WHEN foi.special_offer_id != 1
                THEN 1
                ELSE 0
            END AS has_discount
    FROM
        dev_adventure_works.fact_order_item foi
    JOIN
        dev_adventure_works.dim_product p ON foi.product_id = p.product_id
    LEFT JOIN
        dev_adventure_works.dim_sales_territory st ON foi.sales_territory_id = st.sales_territory_id
    LEFT JOIN
        dev_adventure_works.dim_customer c ON foi.customer_id = c.customer_id
    LEFT JOIN
        dev_adventure_works.dim_store s ON foi.store_id = s.store_id
    LEFT JOIN
        dev_adventure_works.dim_special_offer dimso ON foi.special_offer_id = dimso.special_offer_id
    LEFT JOIN
        first_purchase_check fp ON foi.customer_id = fp.customer_id AND foi.product_id = fp.product_id
    GROUP BY
        DATE_TRUNC(foi.order_date, WEEK(MONDAY))
        , p.product_id
        , p.product_name
        , p.product_category_name
        , p.product_subcategory_name
        , p.product_line
        , p.class
        , p.style
        , p.make_flag
        , p.safety_stock_level
        , p.reorder_point
        , p.days_to_manufacture
        , p.sell_start_date
        , p.sell_end_date
        , p.finished_goods_flag
        , st.sales_territory_name
        , c.city_district_name
        , c.state_province_name
        , s.store_name
        , foi.unit_price
        , foi.unit_price_discount
        , s.store_id
        , foi.online_order_flag
        , foi.ship_method_name
        , foi.sales_person_id
        , foi.sales_reason_names
        , foi.sales_reason_types
        , dimso.discount_pct
        , fp.first_order_date
        , foi.special_offer_id
)

SELECT
    ws.*
FROM
    weekly_sales ws
ORDER BY
    ws.product_id
    , ws.week_start_date
