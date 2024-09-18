{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the employee data and enforces the foreign key constraint on businessentityid which is also the primary key.'
) }}

with stg_employee as (
    select
        cast(businessentityid as int64) as business_entity_id
        , jobtitle as job_title
    from
        {{ source('stg_adventure_works', 'employee') }}
)

select
    *
from
    stg_employee
order by
    business_entity_id
