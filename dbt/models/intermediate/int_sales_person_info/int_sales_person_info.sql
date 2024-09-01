{{ config(
    materialized='view',
    schema='dev_adventure_works'
) }}

with int_sales_person_info as (
    select
        sp.sales_person_id
        , sp.business_entity_id
        , p.first_name
        , p.last_name
        , sp.sales_territory_id
        , st.territory_name
        , st.country_region_id
        , sp.sales_quota
        , sp.bonus
        , sp.commission_pct
        , sp.sales_ytd
        , sp.sales_last_year
    from {{ ref('stg_sales_person') }} sp
    left join {{ ref('stg_person') }} p
        on sp.business_entity_id = p.business_entity_id
    left join {{ ref('stg_sales_territory') }} st
        on sp.sales_territory_id = st.sales_territory_id
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
from int_sales_person_info
order by sales_person_id
