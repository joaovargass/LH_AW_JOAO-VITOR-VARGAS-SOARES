{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the transaction history archive data, ensuring data consistency and validating the primary key and key numerical fields.'
) }}

with stg_transaction_history_archive as (
    select
          cast(tha.transactionid as int64) as transaction_id
        , cast(tha.productid as int64) as product_id
        , cast(tha.referenceorderid as int64) as reference_order_id
        , cast(tha.referenceorderlineid as int64) as reference_order_line_id
        , cast(tha.transactiondate as datetime) as transaction_date
        , tha.transactiontype as transaction_type
        , cast(tha.quantity as int64) as quantity
        , cast(tha.actualcost as float64) as actual_cost
        , cast(tha.modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'transactionhistoryarchive') }} tha
)

select
      transaction_id
    , product_id
    , reference_order_id
    , reference_order_line_id
    , transaction_date
    , transaction_type
    , quantity
    , actual_cost
    , last_modified_date
from
    stg_transaction_history_archive
order by
    transaction_id
