{{ config(
    materialized = 'table',
    schema = 'dev_adventure_works'
) }}

with dim_product as (
    select
        product_id,
        product_name,
        product_number,
        make_flag,
        safety_stock_level,
        reorder_point,
        days_to_manufacture,
        product_category_id,
        product_category_name,
        product_subcategory_id,
        product_subcategory_name,
        product_model_id,
        product_model_name,
        product_line,
        class,
        style,
        color,
        size,
        size_unit_measure_name,
        weight,
        weight_unit_measure_name,
        standard_cost,
        list_price,
        sell_start_date,
        sell_end_date,
        discontinued_date,
        finished_goods_flag
    from {{ ref('int_product_info') }}
)

select *
from dim_product
order by product_id
