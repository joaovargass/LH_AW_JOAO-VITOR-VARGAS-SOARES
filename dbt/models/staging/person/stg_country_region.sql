{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the countryregion data and adds a new primary key.'
) }}

with stg_country_region as (
    select
          row_number() over (order by countryregioncode) as country_region_id
        , countryregioncode as country_region_code
        , name as country_region_name
    from
        {{ source('stg_adventure_works', 'countryregion') }}
)

select
    *
from
    stg_country_region
