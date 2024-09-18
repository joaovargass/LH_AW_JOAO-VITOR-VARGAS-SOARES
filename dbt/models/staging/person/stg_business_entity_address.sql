{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the businessentityaddress data, renames columns to snake_case, and enforces composite primary key and foreign key constraints.'
) }}

with stg_business_entity_address as (
    select
          cast(addressid as int64) as address_id
        , cast(businessentityid as int64) as business_entity_id
        , cast(addresstypeid as int64) as address_type_id
    from
        {{ source('stg_adventure_works', 'businessentityaddress') }}
)

select
    *
from
    stg_business_entity_address
