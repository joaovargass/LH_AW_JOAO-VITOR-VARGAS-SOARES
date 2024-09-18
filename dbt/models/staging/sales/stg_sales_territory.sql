{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the sales territory data, ensuring data consistency and validating key constraints, unique identifiers, and relationships.'
) }}

with stg_sales_territory as (
    select
        st.territoryid as sales_territory_id
        , cast(st.name as string) as sales_territory_name
        , cr.country_region_id as country_region_id
        , cast(st.group as string) as territory_group
        , cast(st.salesytd as numeric) as sales_ytd
        , cast(st.saleslastyear as numeric) as sales_last_year
        , cast(st.costytd as numeric) as cost_ytd
        , cast(st.costlastyear as numeric) as cost_last_year
    from
        {{ source('stg_adventure_works', 'salesterritory') }} st
    left join
        {{ ref('stg_country_region') }} as cr on st.countryregioncode = cr.country_region_code
)

select
    *
from
    stg_sales_territory
order by
    sales_territory_id
