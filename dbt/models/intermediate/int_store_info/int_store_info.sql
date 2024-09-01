{{ config(
    materialized='view',
    schema='dev_adventure_works'
) }}

with int_store_info as (
    select
        distinct s.store_id
        , s.business_entity_id
        , s.store_name
        , s.sales_person_id
        , s.demographics
        , a.state_province_id
        , sp.state_province_name
        , a.city_district_id
        , cd.city_district_name
        , a.country_region_id
        , cr.country_region_name
    from {{ ref('stg_store') }} s
    left join {{ ref('stg_business_entity') }} be
        on s.business_entity_id = be.business_entity_id
    left join {{ ref('stg_business_entity_address') }} bea
        on be.business_entity_id = bea.business_entity_id
    left join {{ ref('stg_address') }} a
        on bea.address_id = a.address_id
    left join {{ ref('stg_state_province') }} sp
        on a.state_province_id = sp.state_province_id
    left join {{ ref('stg_city_district') }} cd
        on a.city_district_id = cd.city_district_id
    left join {{ ref('stg_country_region') }} cr
        on a.country_region_id = cr.country_region_id
)

select
    store_id
    , business_entity_id
    , store_name
    , sales_person_id
    , demographics
    , state_province_id
    , state_province_name
    , city_district_id
    , city_district_name
    , country_region_id
    , country_region_name
from int_store_info
order by store_id
