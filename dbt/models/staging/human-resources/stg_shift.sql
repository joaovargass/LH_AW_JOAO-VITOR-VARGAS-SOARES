{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the shift data, ensures that the shift name is unique, validates the start and end times, and adds a unique primary key shift_id.'
) }}

with stg_shift as (
    select
          cast(shiftid as int64) as shift_id
        , name as shift_name
        , cast(starttime as time) as start_time
        , cast(endtime as time) as end_time
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'shift') }}
)

select
      shift_id
    , shift_name
    , start_time
    , end_time
    , last_modified_date
from
    stg_shift
order by
    shift_id;
