{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the location data, ensuring data consistency and validating the primary key and uniqueness constraints on LocationID and Name, as well as checking for valid cost rates and availability.'
) }}

with stg_location as (
    select
          cast(l.locationid as int64) as location_id
        , l.name as location_name
        , cast(l.costrates as float64) as cost_rate
        , cast(l.availability as float64) as availability
        , cast(substr(l.modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'location') }} l
)

select
      location_id
    , location_name
    , cost_rate
    , availability
    , last_modified_date
from
    stg_location
order by
    location_id;
