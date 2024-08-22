{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model creates a standardized table for city districts, linking to country_region and state_province tables.'
) }}

with stg_city_district as (
    select
          initcap(lower(trim(a.city))) as city_district_name
        , cast(cr.country_region_id as int64) as country_region_id
        , cast(sp.stateprovinceid as int64) as state_province_id
    from
        {{ source('stg_adventure_works', 'address') }} a
    left join
        {{ source('stg_adventure_works', 'stateprovince') }} sp on a.stateprovinceid = sp.stateprovinceid
    left join
        {{ ref('stg_country_region') }} cr on sp.countryregioncode = cr.country_region_code
    group by
          city_district_name
        , country_region_id
        , state_province_id
)

select
      cast(row_number() over (order by city_district_name) as int64) as city_district_id
    , city_district_name
    , country_region_id
    , state_province_id
from
    stg_city_district;
