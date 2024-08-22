{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the contact_type data, converts modifieddate to datetime, renames columns to snake_case, and enforces primary key and unique constraints.'
) }}

with stg_contact_type as (
    select
          cast(contacttypeid as int64) as contact_type_id
        , name as contact_type_name
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'contacttype') }}
)

select
      contact_type_id
    , contact_type_name
    , last_modified_date
from
    stg_contact_type;
