{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the stateprovince data, replaces country_region_code with country_region_id, and enforces primary key and foreign key constraints.'
) }}

with stg_state_province as (
    select
          cast(sp.stateprovinceid as int64) as state_province_id
        , sp.stateprovincecode as state_province_code
        , cr.country_region_id
        , case
            when isonlystateprovinceflag = 't' then true
            else false
        end as is_only_state_province_flag
        , sp.name as state_province_name
        , cast(sp.territoryid as int64) as sales_territory_id
    from
        {{ source('stg_adventure_works', 'stateprovince') }} sp
    left join {{ ref('stg_country_region') }} cr
        on sp.countryregioncode = cr.country_region_code
)

select
    *
from
    stg_state_province
