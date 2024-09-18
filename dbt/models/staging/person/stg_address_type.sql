{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the address_type data, renames columns to snake_case, and enforces primary key and unique constraints.'
) }}

with stg_address_type as (
    select
          cast(addresstypeid as int64) as address_type_id
        , name as address_type_name
    from
        {{ source('stg_adventure_works', 'addresstype') }}
)

select
    *
from
    stg_address_type
