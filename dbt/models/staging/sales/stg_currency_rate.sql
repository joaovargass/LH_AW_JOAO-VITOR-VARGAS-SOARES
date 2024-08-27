{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the currency rate data, ensuring data consistency and validating key constraints and values.'
) }}

with stg_currency_rate as (
    select
        cast(currencyrateid as int64) as currency_rate_id
        , cast(stg_adventure_workscurrencyratedate as datetime) as currency_rate_date
        , cast(fromcurrencycode as string) as from_currency_code
        , cast(tocurrencycode as string) as to_currency_code
        , cast(averagerate as float64) as average_rate
        , cast(endofdayrate as float64) as end_of_day_rate
        , cast(stg_adventure_worksmodifieddate as datetime) as last_modified_date
    from {{ source('stg_adventure_works', 'currencyrate') }}
)

select
    currency_rate_id
    , currency_rate_date
    , from_currency_code
    , to_currency_code
    , average_rate
    , end_of_day_rate
    , last_modified_date
from stg_currency_rate
order by currency_rate_id;
