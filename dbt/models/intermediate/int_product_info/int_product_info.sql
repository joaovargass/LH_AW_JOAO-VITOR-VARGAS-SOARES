{{ config(
    materialized='view',
    schema='dev_adventure_works'
) }}

with int_product_info as (
    select
        p.product_id
        , p.name as product_name
        , p.product_number
        , p.make_flag
        , p.safety_stock_level
        , p.reorder_point
        , p.days_to_manufacture
        , pc.product_category_id
        , pc.product_category_name
        , ps.product_subcategory_id
        , ps.product_subcategory_name
        , p.product_model_id
        , pm.product_model_name
        , p.product_line
        , p.class
        , p.style
        , p.color
        , p.size
        , um_size.unit_measure_name as size_unit_measure_name
        , p.weight
        , um_weight.unit_measure_name as weight_unit_measure_name
        , p.standard_cost
        , p.list_price
        , p.sell_start_date
        , p.sell_end_date
        , p.discontinued_date
        , p.finished_goods_flag
    from {{ ref('stg_product') }} p
    left join {{ ref('stg_product_subcategory') }} ps
        on p.product_subcategory_id = ps.product_subcategory_id
    left join {{ ref('stg_product_category') }} pc
        on ps.product_category_id = pc.product_category_id
    left join {{ ref('stg_product_model') }} pm
        on p.product_model_id = pm.product_model_id
    left join {{ ref('stg_unit_measure') }} um_size
        on p.size_unit_measure_id = um_size.unit_measure_id
    left join {{ ref('stg_unit_measure') }} um_weight
        on p.weight_unit_measure_id = um_weight.unit_measure_id
)

select
    *
from int_product_info
order by product_id
