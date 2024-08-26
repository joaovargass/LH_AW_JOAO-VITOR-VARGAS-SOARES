{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the product data, ensuring data consistency and validating key constraints, date integrity, and value correctness.'
) }}

with stg_unit_measure as (
    select
        unit_measure_code
        , unit_measure_id
    from
        {{ ref('stg_unit_measure') }}
),

stg_product as (
    select
        cast(productid as int64) as product_id
        , name
        , productnumber as product_number
        , case
            when makeflag = 't' then true
            else false
          end as make_flag
        , case
            when finishedgoodsflag = 't' then true
            else false
          end as finished_goods_flag
        , color
        , cast(safetystocklevel as int64) as safety_stock_level
        , cast(reorderpoint as int64) as reorder_point
        , cast(standardcost as float64) as standard_cost
        , cast(listprice as float64) as list_price
        , size
        , su.unit_measure_id as size_unit_measure_id
        , wu.unit_measure_id as weight_unit_measure_id
        , cast(weight as float64) as weight
        , cast(daystomanufacture as int64) as days_to_manufacture
        , productline as product_line
        , class
        , style
        , cast(productsubcategoryid as int64) as product_subcategory_id
        , cast(productmodelid as int64) as product_model_id
        , cast(sellstartdate as datetime) as sell_start_date
        , cast(sellenddate as datetime) as sell_end_date
        , cast(null as datetime) as discontinued_date
        , rowguid
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'product') }} p
    left join
        stg_unit_measure su
        on p.sizeunitmeasurecode = su.unit_measure_code
    left join
        stg_unit_measure wu
        on p.weightunitmeasurecode = wu.unit_measure_code
)
select
    product_id
    , name
    , product_number
    , make_flag
    , finished_goods_flag
    , color
    , safety_stock_level
    , reorder_point
    , standard_cost
    , list_price
    , size
    , size_unit_measure_id
    , weight_unit_measure_id
    , weight
    , days_to_manufacture
    , product_line
    , class
    , style
    , product_subcategory_id
    , product_model_id
    , sell_start_date
    , sell_end_date
    , discontinued_date
    , rowguid
    , last_modified_date
from
    stg_product
order by
    product_id;
