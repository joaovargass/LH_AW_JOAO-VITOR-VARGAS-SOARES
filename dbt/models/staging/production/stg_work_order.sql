{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the work order data, ensuring data consistency and validating key constraints, date integrity, and quantity values.'
) }}

with stg_work_order as (
    select
        cast(workorderid as int64) as work_order_id
      , cast(productid as int64) as product_id
      , cast(orderqty as int64) as order_qty
      , cast(scrappedqty as int64) as scrapped_qty
      , cast(startdate as datetime) as start_date
      , cast(enddate as datetime) as end_date
      , cast(duedate as datetime) as due_date
      , cast(scrapreasonid as int64) as scrap_reason_id
      , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'workorder') }}
)

select
    work_order_id
  , product_id
  , order_qty
  , scrapped_qty
  , start_date
  , end_date
  , due_date
  , scrap_reason_id
  , last_modified_date
from
    stg_work_order
order by
    work_order_id
