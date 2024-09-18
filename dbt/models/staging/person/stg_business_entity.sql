{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the businessentity data, renames columns to snake_case, and enforces primary key and unique constraints.'
) }}

with stg_business_entity as (
    select
          cast(businessentityid as int64) as business_entity_id
    from
        {{ source('stg_adventure_works', 'businessentity') }}
)

select
    *
from
    stg_business_entity
