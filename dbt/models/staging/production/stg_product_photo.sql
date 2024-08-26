{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the product photo data, ensuring data consistency and validating key constraints.'
) }}

with stg_product_photo as (
    select
        cast(productphotoid as int64) as product_photo_id
      , thumbnailphoto as thumbnail_photo
      , thumbnailphotofilename as thumbnail_photo_file_name
      , largephoto as large_photo
      , largephotofilename as large_photo_file_name
      , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'productphoto') }}
)

select
    product_photo_id
  , thumbnail_photo
  , thumbnail_photo_file_name
  , large_photo
  , large_photo_file_name
  , last_modified_date
from
    stg_product_photo
order by
    product_photo_id;
