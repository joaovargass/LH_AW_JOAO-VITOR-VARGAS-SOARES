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
    from
        {{ source('stg_adventure_works', 'customer') }}
)

select
    *
from
    stg_customer
order by
    customer_id
