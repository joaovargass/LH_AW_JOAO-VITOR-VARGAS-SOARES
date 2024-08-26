{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the product model description culture data, ensuring data consistency and validating key constraints and uniqueness of the combination of product_model_id, product_description_id, and culture_id.'
) }}

with stg_product_model_description_culture as (
    select
        cast(productmodelid as int64) as product_model_id
      , cast(productdescriptionid as int64) as product_description_id
      , trim(cultureid) as culture_id
      , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'productmodelproductdescriptionculture') }}
)

select
    product_model_id
  , product_description_id
  , culture_id
  , last_modified_date
from
    stg_product_model_description_culture
order by
    product_model_id
  , product_description_id
  , culture_id;
