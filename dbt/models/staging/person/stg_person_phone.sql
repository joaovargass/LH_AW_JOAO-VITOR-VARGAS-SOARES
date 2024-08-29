{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the personphone data, converts modifieddate to datetime, renames columns to snake_case, and enforces composite primary key constraints.'
) }}

with stg_person_phone as (
    select
          cast(businessentityid as int64) as business_entity_id
        , cast(phonenumbertypeid as int64) as phone_number_type_id
        , cast(regexp_replace(phonenumber, r'\D', '') as int64) as phone_number
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'personphone') }}
)

select
      business_entity_id
    , phone_number
    , phone_number_type_id
    , last_modified_date
from
    stg_person_phone
