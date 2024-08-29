{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the businessentity data, converts modifieddate to datetime, renames columns to snake_case, and enforces primary key and unique constraints.'
) }}

with stg_business_entity as (
    select
          cast(businessentityid as int64) as business_entity_id
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
        , cast(rowguid as string) as row_guid
    from
        {{ source('stg_adventure_works', 'businessentity') }}
)

select
      business_entity_id
    , last_modified_date
    , row_guid
from
    stg_business_entity
