{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the currency data, ensuring data consistency and validating key constraints and values.'
) }}

with stg_currency as (
    select
        cast(currencycode as string) as currency_code
        , cast(name as string) as name
    from {{ source('stg_adventure_works', 'currency') }}
)

select
    *
from stg_currency
order by currency_code
