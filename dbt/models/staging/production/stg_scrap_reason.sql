{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the scrap reason data, ensuring data consistency and validating the primary key and unique constraints on scrapreasonid and name.'
) }}

with stg_scrap_reason as (
    select
          cast(sr.scrapreasonid as int64) as scrap_reason_id
        , sr.name as scrap_reason_name
        , cast(substr(sr.modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'scrapreason') }} sr
)

select
      scrap_reason_id
    , scrap_reason_name
    , last_modified_date
from
    stg_scrap_reason
order by
    scrap_reason_id;
