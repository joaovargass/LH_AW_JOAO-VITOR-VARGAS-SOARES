{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the employee data, ensures the uniqueness of nationalidnumber, loginid, and rowguid, and enforces the foreign key constraint on businessentityid which is also the primary key.'
) }}

with stg_employee as (
    select
          cast(businessentityid as int64) as business_entity_id
        , cast(nationalidnumber as string) as national_id_number
        , cast(loginid as string) as login_id
        , jobtitle as job_title
        , cast(birthdate as datetime) as birth_date
        , maritalstatus as marital_status
        , gender as gender
        , cast(hiredate as datetime) as hire_date
        , cast(salariedflag as boolean) as salaried_flag
        , cast(vacationhours as int64) as vacation_hours
        , cast(sickleavehours as int64) as sick_leave_hours
        , cast(currentflag as boolean) as current_flag
        , cast(rowguid as string) as row_guid
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'employee') }}
)

select
      business_entity_id
    , national_id_number
    , login_id
    , job_title
    , birth_date
    , marital_status
    , gender
    , hire_date
    , salaried_flag
    , vacation_hours
    , sick_leave_hours
    , current_flag
    , row_guid
    , last_modified_date
from
    stg_employee
order by
    business_entity_id;
