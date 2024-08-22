{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the emailaddress data, converts modifieddate to datetime, normalizes emails, and applies snake_case naming conventions.'
) }}

with stg_email_address as (
    select
          cast(emailaddressid as int64) as email_address_id
        , cast(businessentityid as int64) as business_entity_id
        , translate(
            lower(emailaddress)
            , 'áéíóúãõâêîôûàèìòùäëïöüñç'
            , 'aeiouaoaeiouaeiounc'
          ) as email_address
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
        , cast(rowguid as string) as row_guid

    from
        {{ source('stg_adventure_works', 'emailaddress') }}
)

select
      email_address_id
    , business_entity_id
    , email_address
    , last_modified_date
    , row_guid
from
    stg_email_address;
