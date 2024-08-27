{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the password data, ensuring data consistency, validating key constraints, and enforcing proper lengths for password hash and salt.'
) }}

with stg_password as (
    select
        cast(businessentityid as int64) as business_entity_id
      , trim(passwordhash) as password_hash
      , trim(passwordsalt) as password_salt
      , cast(rowguid as string) as row_guid
      , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'password') }}
)

select
    business_entity_id
  , password_hash
  , password_salt
  , row_guid
  , last_modified_date
from
    stg_password
order by
    business_entity_id;
