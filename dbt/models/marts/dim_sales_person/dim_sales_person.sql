{{ config(
    materialized = 'table',
    schema = 'dev_adventure_works'
) }}

with dim_sales_person as (
    select
        sales_person_id,
        business_entity_id,
        first_name,
        last_name,
        sales_territory_id,
        sales_territory_name,
        country_region_id,
        sales_quota,
        bonus,
        commission_pct,
        sales_ytd,
        sales_last_year,
        job_title
    from {{ ref('int_sales_person_info') }}
)

select *
from dim_sales_person
order by sales_person_id
