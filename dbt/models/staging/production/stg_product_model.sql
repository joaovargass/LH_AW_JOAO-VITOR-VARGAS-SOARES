{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the product model data, ensuring data consistency and validating key constraints.'
) }}

with stg_product_model as (
    select
        cast(pm.productmodelid as int64) as product_model_id
        , cast(pm.name as string) as product_model_name
    from
        {{ source('stg_adventure_works', 'productmodel') }} pm
)

select
    *
from
    stg_product_model
order by
    product_model_id
