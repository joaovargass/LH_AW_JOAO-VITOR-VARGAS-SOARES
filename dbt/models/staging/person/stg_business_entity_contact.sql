{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the businessentitycontact data, converts modifieddate to datetime, renames columns to snake_case, and enforces composite primary key and foreign key constraints.'
) }}

with stg_business_entity_contact as (
    select
          cast(businessentityid as int64) as business_entity_id
        , cast(personid as int64) as person_id
        , cast(contacttypeid as int64) as contact_type_id
        , cast(rowguid as string) as row_guid
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'businessentitycontact') }}
)

select
      business_entity_id
    , person_id
    , contact_type_id
    , row_guid
    , last_modified_date
from
    stg_business_entity_contact
