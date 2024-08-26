{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the product document data, ensuring data consistency and validating key constraints and uniqueness of the combination of product_id and document_node.'
) }}

with stg_product_document as (
    select
        cast(productid as int64) as product_id
      , cast(documentnode as string) as document_node
      , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'productdocument') }}
)

select
    product_id
  , document_node
  , last_modified_date
from
    stg_product_document
order by
    product_id
  , document_node;
