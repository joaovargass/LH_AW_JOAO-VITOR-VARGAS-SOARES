{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the department data, ensuring that names are capitalized, replaces null values in the GroupName column, and adds a unique primary key department_id.'
) }}

with stg_department as (
    select
          cast(departmentid as int64) as department_id
        , name as department_name
        , groupname as group_name
        , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'department') }}

)

select
      department_id
    , department_name
    , group_name
    , last_modified_date
from
    stg_department
order by
    department_id
