{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the address data, adds country_region_id, extracts and cleans address components, removes the city column, and converts spatial_location to lat/long.'
) }}

with stg_address as (
    select
          cast(a.addressid as int64) as address_id
        , sp.country_region_id
        , a.stateprovinceid as state_province_id
        , cd.city_district_id
        , cast(regexp_extract(a.addressline1, r'\b\d+\b') as int64) as address_number
        , lower(trim(regexp_replace(a.addressline1, r'\b\d+\b|\bno\.?\b|\bn\.?\b|,', ''))) as address_street
        , lower(a.addressline2) as address_complement
        , a.postalcode as postal_code
        , a.spatiallocation as spatial_location
        , rowguid as row_guid
        , cast(substr(a.modifieddate, 1, 19) as datetime) as last_modified_date
    from {{ source('stg_adventure_works', 'address') }} a
    left join {{ ref('stg_state_province') }} sp
        on a.stateprovinceid = sp.state_province_id
    left join {{ ref('stg_city_district') }} cd
        on lower(a.city) = lower(cd.city_district_name)
        and sp.country_region_id = cd.country_region_id
        and a.stateprovinceid = cd.state_province_id
    )
select
      address_id
    , country_region_id
    , state_province_id
    , city_district_id
    , address_number
    , address_street
    , address_complement
    , postal_code
    , spatial_location
    , row_guid
    , last_modified_date
from
    stg_address
order by
    address_id
