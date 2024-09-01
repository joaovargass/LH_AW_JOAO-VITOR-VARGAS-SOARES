{{ config(
    materialized='table',
    schema='dev_adventure_works'
) }}

with sales_person_info as (
    select
          sales_person_id
        , first_name
        , last_name
        , sales_quota
        , bonus
        , commission_pct
        , sales_ytd
        , sales_last_year
        , territory_name
        , country_region_id
    from {{ ref('int_sales_person_info') }}
)

select
      sales_person_id
    , first_name
    , last_name
    , sales_quota
    , bonus
    , commission_pct
    , sales_ytd
    , sales_last_year
    , territory_name
    , country_region_id
from sales_person_info
