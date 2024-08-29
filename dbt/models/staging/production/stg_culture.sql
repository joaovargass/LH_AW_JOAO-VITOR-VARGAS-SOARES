{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the culture data, ensuring data consistency and validating the primary key and unique constraints on cultureid and name.'
) }}

with stg_culture as (
    select
          trim(cast(c.cultureid as string)) as culture_id
        , c.name as culture_name
        , cast(c.modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'culture') }} c
)

select
      culture_id
    , culture_name
    , last_modified_date
from
    stg_culture
order by
    culture_id
