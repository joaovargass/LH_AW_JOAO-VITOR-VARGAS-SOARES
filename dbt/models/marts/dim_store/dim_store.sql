{{ config(
    materialized='table',
    schema='dev_adventure_works'
) }}

with store_info as (
    select
          store_id
        , business_entity_id
        , store_name
        , sales_person_id
        , demographics
        , state_province_name
        , city_district_name
        , country_region_name
    from {{ ref('int_store_info') }}
)

select
      store_id
    , business_entity_id
    , store_name
    , sales_person_id
    , demographics
    , state_province_name
    , city_district_name
    , country_region_name
from store_info
