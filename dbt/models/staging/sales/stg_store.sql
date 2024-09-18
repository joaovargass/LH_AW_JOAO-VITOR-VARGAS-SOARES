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
    from
        {{ source('stg_adventure_works', 'store') }} as s
    left join {{ ref('stg_sales_person') }} as sp
        on cast(s.salespersonid as int) = sp.business_entity_id
)

select
    *
from
    stg_store
order by
    store_id
