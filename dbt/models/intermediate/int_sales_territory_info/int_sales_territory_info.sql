{{ config(
    materialized='view',
    description='''This model transforms the sales territory data by joining the stg_sales_territory and stg_country_region tables to provide detailed information about each sales territory, including financial metrics and region names.'''
) }}

with sales_territory as (
    select
        st.sales_territory_id
        , st.territory_name as sales_territory_name
        , st.territory_group as sales_territory_group
        , st.country_region_id
        , cr.country_region_name
        , st.sales_ytd
        , st.sales_last_year
        , st.cost_ytd
        , st.cost_last_year
    from {{ ref('stg_sales_territory') }} st
    left join {{ ref('stg_country_region') }} cr
      on cr.country_region_id = st.country_region_id
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
from sales_territory
order by sales_territory_id
