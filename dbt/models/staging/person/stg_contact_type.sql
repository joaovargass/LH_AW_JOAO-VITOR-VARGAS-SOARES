{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the contact_type data, renames columns to snake_case, and enforces primary key and unique constraints.'
) }}

with stg_contact_type as (
    select
          cast(contacttypeid as int64) as contact_type_id
        , name as contact_type_name
    from
        {{ source('stg_adventure_works', 'contacttype') }}
)

select
    *
from
    stg_contact_type
