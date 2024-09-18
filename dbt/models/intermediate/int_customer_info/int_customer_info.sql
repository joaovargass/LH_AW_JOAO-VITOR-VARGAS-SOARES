{{ config(
    materialized='view',
    schema='dev_adventure_works',
    description='View that provides detailed customer information, including addresses of type 2 (specific address type) along with their related information such as state, city, and country.'
) }}

with int_customer_info as (
    select
          distinct c.customer_id
        , c.person_id
        , c.store_id
        , c.sales_territory_id
        , p.first_name
        , p.middle_name
        , p.last_name
        , p.name_title
        , p.person_type
        , p.name_suffix
        , p.email_promotion
        , ea.email_address
        , a.state_province_id
        , sp.state_province_name
        , a.city_district_id
        , cd.city_district_name
        , a.country_region_id
        , cr.country_region_name
        , addr_type.address_type_name
    from {{ ref('stg_customer') }} c
    left join {{ ref('stg_person') }} p
        on c.person_id = p.business_entity_id
        and c.person_id is not null
    left join {{ ref('stg_email_address') }} ea
        on c.person_id = ea.business_entity_id
        and c.person_id is not null
    left join {{ ref('stg_business_entity') }} be
        on c.person_id = be.business_entity_id
    left join {{ ref('stg_business_entity_address') }} bea
        on be.business_entity_id = bea.business_entity_id
        and bea.address_type_id = 2
    left join {{ ref('stg_address') }} a
        on bea.address_id = a.address_id
    left join {{ ref('stg_state_province') }} sp
        on a.state_province_id = sp.state_province_id
    left join {{ ref('stg_city_district') }} cd
        on a.city_district_id = cd.city_district_id
    left join {{ ref('stg_country_region') }} cr
        on a.country_region_id = cr.country_region_id
    left join {{ ref('stg_address_type') }} addr_type
        on bea.address_type_id = addr_type.address_type_id
)

select distinct *
from int_customer_info
order by customer_id
