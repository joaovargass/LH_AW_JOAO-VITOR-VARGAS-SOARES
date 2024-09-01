{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the product vendor data, ensuring data consistency and using unit_measure_id as a foreign key, populated from the stg_unit_measure table.'
) }}

with unit_measure_lookup as (
    select
        unit_measure_code,
        unit_measure_id
    from
        {{ ref('stg_unit_measure') }}
),

stg_product_vendor as (
    select
          cast(pv.productid as int64) as product_id
        , cast(pv.businessentityid as int64) as business_entity_id
        , cast(pv.averageleadtime as int64) as average_lead_time
        , cast(pv.standardprice as float64) as standard_price
        , cast(pv.lastreceiptcost as float64) as last_receipt_cost
        , cast(pv.lastreceiptdate as datetime) as last_receipt_date
        , cast(pv.minorderqty as int64) as min_order_qty
        , cast(pv.maxorderqty as int64) as max_order_qty
        , um.unit_measure_id as unit_measure_id
        , cast(substr(pv.modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'productvendor') }} pv
    left join
        unit_measure_lookup um
    on
        trim(pv.unitmeasurecode) = trim(um.unit_measure_code)
)

select
      product_id
    , business_entity_id
    , average_lead_time
    , standard_price
    , last_receipt_cost
    , last_receipt_date
    , min_order_qty
    , max_order_qty
    , unit_measure_id
    , last_modified_date
from
    stg_product_vendor
order by
    product_id, business_entity_id
