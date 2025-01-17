{{ config(
    materialized = 'table',
    schema = 'dev_adventure_works'
) }}

with dim_store as (
    select
        store_id,
        business_entity_id,
        store_name,
        sales_person_id,
        state_province_id,
        state_province_name,
        city_district_id,
        city_district_name,
        country_region_id,
        country_region_name
    from {{ ref('int_store_info') }}
)

select *
from dim_store
order by store_id
