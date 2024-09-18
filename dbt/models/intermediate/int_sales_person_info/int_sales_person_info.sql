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
        , st.sales_territory_name
        , st.country_region_id
        , sp.sales_quota
        , sp.bonus
        , sp.commission_pct
        , sp.sales_ytd
        , sp.sales_last_year
        , e.job_title
    from {{ ref('stg_sales_person') }} sp
    left join {{ ref('stg_person') }} p
        on sp.business_entity_id = p.business_entity_id
    left join {{ ref('stg_sales_territory') }} st
        on sp.sales_territory_id = st.sales_territory_id
    left join {{ ref('stg_employee') }} e
        on sp.business_entity_id = e.business_entity_id
)

select
    *
from int_sales_person_info
order by sales_person_id
