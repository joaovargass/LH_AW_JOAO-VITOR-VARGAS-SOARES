{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the customer data, ensuring data consistency and validating key constraints, unique identifiers, and relationships.'
) }}

with stg_customer as (
    select
        cast(customerid as int64) as customer_id
        , cast(personid as int64) as person_id
        , cast(storeid as int64) as store_id
        , cast(territoryid as int64) as sales_territory_id
        , cast(rowguid as string) as row_guid
        , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'customer') }}
)

select
    customer_id
    , person_id
    , store_id
    , sales_territory_id
    , row_guid
    , last_modified_date
from
    stg_customer
order by
    customer_id
