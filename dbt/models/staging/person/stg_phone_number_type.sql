{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the phonenumbertype data, converts modifieddate to datetime, renames columns to snake_case, and enforces primary key and unique constraints.'
) }}

with stg_phone_number_type as (
    select
          cast(phonenumbertypeid as int64) as phone_number_type_id
        , name as phone_number_type_name
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'phonenumbertype') }}
)

select
      phone_number_type_id
    , phone_number_type_name
    , last_modified_date
from
    stg_phone_number_type
