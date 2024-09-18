{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the product category data, ensuring data consistency and validating key constraints and uniqueness of product_category_name.'
) }}

with stg_product_category as (
    select
        cast(productcategoryid as int64) as product_category_id
      , trim(name) as product_category_name
    from
        {{ source('stg_adventure_works', 'productcategory') }}
)

select
    *
from
    stg_product_category
order by
    product_category_id
