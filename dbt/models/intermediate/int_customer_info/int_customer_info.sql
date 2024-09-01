{{ config(
    materialized='view',
    schema='dev_adventure_works'
) }}

with int_customer_info as (
    select distinct
        c.customer_id
        , c.person_id
        , p.first_name
        , p.middle_name
        , p.last_name
        , p.name_title
        , p.name_style
        , p.person_type
        , p.name_suffix
        , p.email_promotion
        , p.additional_contact_info
        , p.demographics
        , c.store_id
        , c.sales_territory_id
        , cr.country_region_id
        , cr.country_region_name
    from {{ ref('stg_customer') }} c
    left join {{ ref('stg_person') }} p
        on c.person_id = p.business_entity_id
    left join {{ ref('stg_sales_territory') }} st
        on c.sales_territory_id = st.sales_territory_id
    left join {{ ref('stg_country_region') }} cr
        on st.country_region_id = cr.country_region_id
)

select distinct
    customer_id
    , person_id
    , first_name
    , middle_name
    , last_name
    , name_title
    , name_style
    , person_type
    , name_suffix
    , email_promotion
    , additional_contact_info
    , demographics
    , store_id
    , sales_territory_id
    , country_region_id
    , country_region_name
from int_customer_info
order by customer_id
