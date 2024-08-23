{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the vendor data, ensures the uniqueness of accountnumber, and enforces the foreign key constraint on businessentityid which is also the primary key.'
) }}

with stg_vendor as (
    select
          cast(v.businessentityid as int64) as business_entity_id
        , cast(v.accountnumber as string) as account_number
        , v.name as vendor_name
        , cast(v.creditrating as int64) as credit_rating
        , cast(v.preferredvendorstatus as boolean) as preferred_vendor_status
        , cast(v.activeflag as boolean) as active_flag
        , v.purchasingwebserviceurl as purchasing_web_service_url
        , cast(substr(v.modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'vendor') }} v
)

select
      business_entity_id
    , account_number
    , vendor_name
    , credit_rating
    , preferred_vendor_status
    , active_flag
    , purchasing_web_service_url
    , last_modified_date
from
    stg_vendor
order by
    business_entity_id;
