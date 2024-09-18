{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the product subcategory data, ensuring data consistency and validating key constraints and uniqueness of product_subcategory_name.'
) }}

with stg_product_subcategory as (
    select
        cast(productsubcategoryid as int64) as product_subcategory_id
      , cast(productcategoryid as int64) as product_category_id
      , trim(name) as product_subcategory_name
    from
        {{ source('stg_adventure_works', 'productsubcategory') }}
)

select
    *
from
    stg_product_subcategory
order by
    product_subcategory_id
