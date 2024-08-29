{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the stateprovince data, replaces country_region_code with country_region_id, converts modifieddate to datetime, and enforces primary key and foreign key constraints.'
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
        , cast(sp.rowguid as string) as row_guid
        , cast(substr(sp.modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'stateprovince') }} sp
    left join {{ ref('stg_country_region') }} cr
        on sp.countryregioncode = cr.country_region_code
)

select
      state_province_id
    , state_province_code
    , country_region_id
    , is_only_state_province_flag
    , state_province_name
    , sales_territory_id
    , row_guid
    , last_modified_date
from
    stg_state_province
