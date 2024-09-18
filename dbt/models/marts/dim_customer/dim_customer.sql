{{ config(
    materialized = 'table',
    schema = 'dev_adventure_works'
) }}

with dim_customer as (
    select
        customer_id,
        person_id,
        store_id,
        sales_territory_id,
        first_name,
        middle_name,
        last_name,
        name_title,
        person_type,
        name_suffix,
        email_promotion,
        email_address,
        state_province_id,
        state_province_name,
        city_district_id,
        city_district_name,
        country_region_id,
        country_region_name,
        address_type_name
    from {{ ref('int_customer_info') }}
)

select *
from dim_customer
order by customer_id
