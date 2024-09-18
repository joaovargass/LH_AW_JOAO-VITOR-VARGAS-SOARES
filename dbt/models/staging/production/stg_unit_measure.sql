{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the unit measure data, creating a new unique identifier unit_measure_id and ensuring data consistency by validating the uniqueness and non-null constraint on unit_measure_code.'
) }}

with stg_unit_measure as (
    select
          cast(row_number() over (order by trim(unitmeasurecode)) as int64) as unit_measure_id
        , trim(unitmeasurecode) as unit_measure_code
        , name as unit_measure_name
    from
        {{ source('stg_adventure_works', 'unitmeasure') }}
)

select
      unit_measure_id
    , unit_measure_code
    , unit_measure_name
from
    stg_unit_measure
order by
    unit_measure_id
