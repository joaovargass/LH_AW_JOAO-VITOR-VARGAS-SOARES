{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='this model standardizes the product list price history data, ensuring data consistency, validating key constraints, and ensuring no negative list prices.'
) }}

with stg_product_list_price_history as (
    select
        row_number() over (order by productid, startdate) as product_list_price_history_id
      , cast(productid as int64) as product_id
      , cast(startdate as datetime) as start_date
      , cast(enddate as datetime) as end_date
      , cast(listprice as float64) as list_price
      , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'productlistpricehistory') }}
)

select
    product_list_price_history_id
  , product_id
  , start_date
  , end_date
  , list_price
  , last_modified_date
from
    stg_product_list_price_history
order by
    product_list_price_history_id;
