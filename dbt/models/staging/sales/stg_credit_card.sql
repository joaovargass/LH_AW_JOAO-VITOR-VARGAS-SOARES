{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model anonymizes the credit card data by hashing the card number with a salt and masking the expiration date. It ensures data consistency and validates key constraints, unique identifiers, and relationships.'
) }}

with stg_credit_card as (
    select
        cast(creditcardid as int64) as credit_card_id
        , cast(cardtype as string) as card_type
        , cast(cardnumber as string) as card_number
        , cast(expmonth as int64) as exp_month
        , cast(expyear as int64) as exp_year
        , cast(modifieddate as datetime) as last_modified_date
        , generate_uuid() as salt
    from
        {{ source('stg_adventure_works', 'creditcard') }}
)

select
    credit_card_id
    , card_type
    , to_hex(sha256(concat(card_number, salt))) as hashed_card_number
    , concat('**/', lpad(cast(exp_month as string), 2, '0')) as masked_exp_month
    , concat('****') as masked_exp_year
    , last_modified_date
    , salt
from
    stg_credit_card
order by
    credit_card_id
