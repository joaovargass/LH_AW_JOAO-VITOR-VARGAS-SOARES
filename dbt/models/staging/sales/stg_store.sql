{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the store data, ensuring data consistency and validating key constraints, unique identifiers, and relationships.'
) }}

with stg_store as (
    select
        row_number() over (order by businessentityid) as store_id
        , cast(businessentityid as int64) as business_entity_id
        , cast(name as string) as store_name
        , cast(salespersonid as int64) as sales_person_id
        , cast(demographics as string) as demographics
        , cast(rowguid as string) as row_guid
        , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'store') }}
)

select
    store_id
    , business_entity_id
    , store_name
    , sales_person_id
    , demographics
    , row_guid
    , last_modified_date
from
    stg_store
order by
    store_id;
