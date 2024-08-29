{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the emailaddress data, converts modifieddate to datetime, normalizes emails, and applies snake_case naming conventions.'
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
        , cast(substr(e.modifieddate, 1, 19) as datetime) as last_modified_date
        , cast(e.rowguid as string) as row_guid

    from
        {{ source('stg_adventure_works', 'emailaddress') }} as e
)

select
      email_address_id
    , business_entity_id
    , email_address
    , last_modified_date
    , row_guid
from
    stg_email_address
