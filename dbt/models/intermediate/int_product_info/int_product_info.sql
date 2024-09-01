{{ config(
    materialized='view',
    schema='dev_adventure_works'
) }}

with int_product_info as (
    select
        p.product_id
        , p.name as product_name
        , p.product_number
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
    product_id
    , product_name
    , product_number
    , product_category_id
    , product_category_name
    , product_subcategory_id
    , product_subcategory_name
    , product_model_id
    , product_model_name
    , product_line
    , class
    , style
    , color
    , size
    , size_unit_measure_name
    , weight
    , weight_unit_measure_name
    , standard_cost
    , list_price
    , sell_start_date
    , sell_end_date
    , discontinued_date
    , finished_goods_flag
from int_product_info
order by product_id
