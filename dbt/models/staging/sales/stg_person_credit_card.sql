{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the person credit card data, ensuring data consistency and validating key constraints, unique identifiers, and relationships.'
) }}

with stg_person_credit_card as (
    select
        cast(businessentityid as int64) as business_entity_id
        , cast(creditcardid as int64) as credit_card_id
        , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'personcreditcard') }}
)

select
    business_entity_id
    , credit_card_id
    , last_modified_date
from
    stg_person_credit_card
order by
    business_entity_id
    , credit_card_id
