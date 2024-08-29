{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the store data, ensuring data consistency and validating key constraints, unique identifiers, and relationships.'
) }}

with stg_store as (
    select
        row_number() over (order by s.businessentityid) as store_id
        , cast(s.businessentityid as int64) as business_entity_id
        , cast(s.name as string) as store_name
        , sp.sales_person_id
        , cast(s.demographics as string) as demographics
        , cast(s.rowguid as string) as row_guid
        , cast(s.modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'store') }} as s
    left join {{ ref('stg_sales_person') }} as sp
        on cast(s.salespersonid as int) = sp.business_entity_id
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
    store_id
