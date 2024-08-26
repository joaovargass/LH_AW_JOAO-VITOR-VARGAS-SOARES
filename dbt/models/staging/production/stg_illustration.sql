{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the illustration data, ensuring data consistency and validating the primary key on illustrationid.'
) }}

with stg_illustration as (
    select
          cast(i.illustrationid as int64) as illustration_id
        , i.diagram as illustration_diagram
        , cast(substr(i.modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'illustration') }} i
)

select
      illustration_id
    , illustration_diagram
    , last_modified_date
from
    stg_illustration
order by
    illustration_id;
