{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the product review data, ensuring data consistency, validating key constraints, and enforcing proper email and rating formats.'
) }}

with stg_product_review as (
    select
        cast(productreviewid as int64) as product_review_id
      , cast(productid as int64) as product_id
      , trim(reviewername) as reviewer_name
      , cast(reviewdate as datetime) as review_date
      , trim(emailaddress) as email_address
      , cast(rating as int64) as rating
      , trim(comments) as comments
      , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'productreview') }}
)

select
    product_review_id
  , product_id
  , reviewer_name
  , review_date
  , email_address
  , rating
  , comments
  , last_modified_date
from
    stg_product_review
order by
    product_review_id
