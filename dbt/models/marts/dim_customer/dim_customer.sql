{{ config(
    materialized = 'table'
    , schema = 'dev_adventure_works'
) }}

with customer_info as (
    select
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
    from {{ ref('int_customer_info') }}
)

select
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
from customer_info
