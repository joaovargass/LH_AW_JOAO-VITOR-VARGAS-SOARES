{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the work order routing data, ensuring data consistency and validating key constraints, date integrity, and cost and resource usage.'
) }}

with stg_work_order_routing as (
    select
        cast(workorderid as int64) as work_order_id
      , cast(productid as int64) as product_id
      , cast(operationsequence as int64) as operation_sequence
      , cast(locationid as int64) as location_id
      , cast(scheduledstartdate as datetime) as scheduled_start_date
      , cast(scheduledenddate as datetime) as scheduled_end_date
      , cast(actualstartdate as datetime) as actual_start_date
      , cast(actualenddate as datetime) as actual_end_date
      , cast(actualresourcehrs as float64) as actual_resource_hrs
      , cast(plannedcost as float64) as planned_cost
      , cast(actualcost as float64) as actual_cost
      , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'workorderrouting') }}
)

select
    work_order_id
  , product_id
  , operation_sequence
  , location_id
  , scheduled_start_date
  , scheduled_end_date
  , actual_start_date
  , actual_end_date
  , actual_resource_hrs
  , planned_cost
  , actual_cost
  , last_modified_date
from
    stg_work_order_routing
order by
    work_order_id
  , product_id
  , operation_sequence;
