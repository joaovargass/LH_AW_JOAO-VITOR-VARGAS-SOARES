{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the purchase order detail data, ensuring data consistency and enforcing the foreign key constraints on purchaseorderid and productid.'
) }}

with stg_purchase_order_detail as (
    select
          cast(pod.purchaseorderid as int64) as purchase_order_id
        , cast(pod.purchaseorderdetailid as int64) as purchase_order_detail_id
        , cast(pod.duedate as datetime) as due_date
        , cast(pod.orderqty as int64) as order_qty
        , cast(pod.productid as int64) as product_id
        , cast(pod.unitprice as float64) as unit_price
        , cast(pod.receivedqty as int64) as received_qty
        , cast(pod.rejectedqty as int64) as rejected_qty
        , cast(substr(pod.modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'purchaseorderdetail') }} pod
)

select
      purchase_order_id
    , purchase_order_detail_id
    , due_date
    , order_qty
    , product_id
    , unit_price
    , received_qty
    , rejected_qty
    , last_modified_date
from
    stg_purchase_order_detail
order by
    purchase_order_id
    , purchase_order_detail_id
