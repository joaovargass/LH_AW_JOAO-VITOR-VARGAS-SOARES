{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the emailaddress data, normalizes emails, and applies snake_case naming conventions.'
) }}

with stg_email_address as (
    select
          cast(e.emailaddressid as int64) as email_address_id
        , cast(e.businessentityid as int64) as business_entity_id
        , translate(
            lower(cast(e.emailaddress as string))
            , 'áéíóúãõâêîôûàèìòùäëïöüñç'
            , 'aeiouaoaeiouaeiounc'
          ) as email_address
    from
        {{ source('stg_adventure_works', 'emailaddress') }} as e
)

select
    *
from
    stg_email_address
