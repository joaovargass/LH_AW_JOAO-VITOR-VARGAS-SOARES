{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the product description data, ensuring data consistency and validating the primary key and unique constraints on productdescriptionid and rowguid.'
) }}

with stg_product_description as (
    select
          cast(pd.productdescriptionid as int64) as product_description_id
        , pd.description as product_description
        , cast(pd.rowguid as string) as row_guid
        , cast(substr(pd.modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'productdescription') }} pd
)

select
      product_description_id
    , product_description
    , row_guid
    , last_modified_date
from
    stg_product_description
order by
    product_description_id
