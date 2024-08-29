{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model creates a new primary key column employee_pay_history_id, ensures that rate is a float, sets ratechangedate as unique, and establishes businessentityid as a foreign key referencing the employee table.'
) }}

with stg_employee_pay_history as (
    select
          cast(row_number() over (order by businessentityid, ratechangedate) as int64) as employee_pay_history_id
        , cast(businessentityid as int64) as business_entity_id
        , cast(ratechangedate as datetime) as rate_change_date
        , cast(rate as float64) as rate
        , cast(payfrequency as int64) as pay_frequency
        , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'employeepayhistory') }}
)

select
      employee_pay_history_id
    , business_entity_id
    , rate_change_date
    , rate
    , pay_frequency
    , last_modified_date
from
    stg_employee_pay_history
order by
    employee_pay_history_id
