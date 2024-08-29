{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the product model illustration data, ensuring data consistency and validating key constraints and date integrity.'
) }}

with stg_product_model_illustration as (
    select
          cast(pmi.productmodelid as int64) as product_model_id
        , cast(pmi.illustrationid as int64) as illustration_id
        , cast(substr(pmi.modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'productmodelillustration') }} pmi
)

select
      product_model_id
    , illustration_id
    , last_modified_date
from
    stg_product_model_illustration
order by
    product_model_id,
    illustration_id
