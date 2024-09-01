{{ config(
    materialized='table',
    schema='dev_adventure_works'
) }}

with sales_territory_info as (
    select
          sales_territory_id
        , sales_territory_name
        , sales_territory_group
        , country_region_id
        , country_region_name
        , sales_ytd
        , sales_last_year
        , cost_ytd
        , cost_last_year
    from {{ ref('int_sales_territory_info') }}
)

select
      sales_territory_id
    , sales_territory_name
    , sales_territory_group
    , country_region_id
    , country_region_name
    , sales_ytd
    , sales_last_year
    , cost_ytd
    , cost_last_year
from sales_territory_info
