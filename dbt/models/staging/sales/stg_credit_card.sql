{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the credit card data, ensuring data consistency and validating key constraints, unique identifiers, and relationships.'
) }}

with stg_credit_card as (
    select
        cast(creditcardid as int64) as credit_card_id
        , cast(cardtype as string) as card_type
        , cast(cardnumber as string) as card_number
        , cast(expmonth as int64) as exp_month
        , cast(expyear as int64) as exp_year
        , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'creditcard') }}
)

select
    credit_card_id
    , card_type
    , card_number
    , exp_month
    , exp_year
    , last_modified_date
from
    stg_credit_card
order by
    credit_card_id;
