{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the product inventory data, ensuring data consistency and validating key constraints and quantity values.'
) }}

with stg_product_inventory as (
    select
        cast(productid as int64) as product_id
      , cast(locationid as int64) as location_id
      , trim(shelf) as shelf
      , cast(bin as int64) as bin
      , cast(quantity as int64) as quantity
      , cast(rowguid as string) as row_guid
      , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'productinventory') }}
)

select
    product_id
  , location_id
  , shelf
  , bin
  , quantity
  , row_guid
  , last_modified_date
from
    stg_product_inventory
order by
    product_id
  , location_id;
