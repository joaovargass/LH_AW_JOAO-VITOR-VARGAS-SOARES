{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the currency data, ensuring data consistency and validating key constraints and values.'
) }}

with stg_currency as (
    select
        cast(currencycode as string) as currency_code
        , cast(name as string) as name
        , cast(modifieddate as datetime) as last_modified_date
    from {{ source('stg_adventure_works', 'currency') }}
)

select
    currency_code
    , name
    , last_modified_date
from stg_currency
order by currency_code
