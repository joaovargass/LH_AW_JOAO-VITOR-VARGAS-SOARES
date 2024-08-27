{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model creates a new primary key column employee_department_history_id, transforms dates, ensures key constraints, and sets startdate as non-nullable.'
) }}

with stg_employee_department_history as (
    select
          cast(row_number() over (
            order by
                  business_entity_id
                , start_date
                , department_id
                , shift_id
                ) as int64
            ) as employee_department_history_id
        , cast(businessentityid as int64) as business_entity_id
        , cast(departmentid as int64) as department_id
        , cast(shiftid as int64) as shift_id
        , cast(startdate as datetime) as start_date
        , cast(enddate as datetime) as end_date
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'employeedepartmenthistory') }}
)

select
      employee_department_history_id
    , business_entity_id
    , department_id
    , shift_id
    , start_date
    , end_date
    , last_modified_date
from
    stg_employee_department_history
order by
    employee_department_history_id;
