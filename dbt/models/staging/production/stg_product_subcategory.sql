{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the product subcategory data, ensuring data consistency and validating key constraints and uniqueness of product_subcategory_name and row_guid.'
) }}

with stg_product_subcategory as (
    select
        cast(productsubcategoryid as int64) as product_subcategory_id
      , cast(productcategoryid as int64) as product_category_id
      , trim(name) as product_subcategory_name
      , cast(rowguid as string) as row_guid
      , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'productsubcategory') }}
)

select
    product_subcategory_id
  , product_category_id
  , product_subcategory_name
  , row_guid
  , last_modified_date
from
    stg_product_subcategory
order by
    product_subcategory_id
