{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the sales person data, ensuring data consistency and validating key constraints, unique identifiers, and relationships.'
) }}

with stg_sales_person as (
    select
        row_number() over (order by businessentityid) as sales_person_id
        , cast(businessentityid as int64) as business_entity_id
        , cast(territoryid as int64) as sales_territory_id
        , cast(salesquota as numeric) as sales_quota
        , cast(bonus as numeric) as bonus
        , cast(commissionpct as numeric) as commission_pct
        , cast(salesytd as numeric) as sales_ytd
        , cast(saleslastyear as numeric) as sales_last_year
        , cast(rowguid as string) as row_guid
        , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'salesperson') }}
)

select
    sales_person_id
    , business_entity_id
    , sales_territory_id
    , sales_quota
    , bonus
    , commission_pct
    , sales_ytd
    , sales_last_year
    , row_guid
    , last_modified_date
from
    stg_sales_person
order by
    sales_person_id
