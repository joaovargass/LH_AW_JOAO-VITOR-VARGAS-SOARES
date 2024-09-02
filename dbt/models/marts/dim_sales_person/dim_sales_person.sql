{{ config(
    materialized = 'table'
    , schema = 'dev_adventure_works'
) }}

with sales_person_info as (
    select
          sales_person_id
        , business_entity_id
        , first_name
        , last_name
        , sales_territory_id
        , territory_name
        , country_region_id
        , sales_quota
        , bonus
        , commission_pct
        , sales_ytd
        , sales_last_year
    from {{ ref('int_sales_person_info') }}
)

select
      sales_person_id
    , business_entity_id
    , first_name
    , last_name
    , sales_territory_id
    , territory_name
    , country_region_id
    , sales_quota
    , bonus
    , commission_pct
    , sales_ytd
    , sales_last_year
from sales_person_info
