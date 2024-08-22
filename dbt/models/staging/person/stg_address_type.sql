{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the address_type data, converts modifieddate to datetime, renames columns to snake_case, and enforces primary key and unique constraints.'
) }}

with stg_address_type as (
    select
          cast(addresstypeid as int64) as address_type_id
        , name as address_type_name
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
        , cast(rowguid as string) as row_guid
    from
        {{ source('stg_adventure_works', 'addresstype') }}
)

select
      address_type_id
    , address_type_name
    , last_modified_date
    , row_guid
from
    stg_address_type;
