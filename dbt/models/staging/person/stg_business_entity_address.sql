{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the businessentityaddress data, converts modifieddate to datetime, renames columns to snake_case, and enforces composite primary key and foreign key constraints.'
) }}

with stg_business_entity_address as (
    select
          cast(addressid as int64) as address_id
        , cast(businessentityid as int64) as business_entity_id
        , cast(addresstypeid as int64) as address_type_id
        , cast(rowguid as string) as row_guid
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'businessentityaddress') }}
)

select
      address_id
    , business_entity_id
    , address_type_id
    , row_guid
    , last_modified_date
from
    stg_business_entity_address
