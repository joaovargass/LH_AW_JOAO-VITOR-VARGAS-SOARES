{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the transaction history data, ensuring data consistency and validating key constraints, date integrity, and non-negative quantities and costs.'
) }}

with stg_transaction_history as (
    select
          cast(th.transactionid as int64) as transaction_id
        , cast(th.productid as int64) as product_id
        , cast(th.referenceorderid as int64) as reference_order_id
        , cast(th.referenceorderlineid as int64) as reference_order_line_id
        , cast(substr(th.transactiondate, 1, 19) as datetime) as transaction_date
        , th.transactiontype as transaction_type
        , cast(th.quantity as int64) as quantity
        , cast(th.actualcost as float64) as actual_cost
        , cast(substr(th.modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'transactionhistory') }} th
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
    stg_transaction_history
order by
    transaction_id;
