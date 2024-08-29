{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the purchase order header data, ensuring data consistency and enforcing the foreign key constraints on vendorid, employeeid, and shipmethodid.'
) }}

with stg_purchase_order_header as (
    select
          cast(poh.purchaseorderid as int64) as purchase_order_id
        , cast(poh.revisionnumber as int64) as revision_number
        , cast(poh.status as int64) as status
        , cast(poh.employeeid as int64) as employee_id
        , cast(poh.vendorid as int64) as vendor_id
        , cast(poh.shipmethodid as int64) as ship_method_id
        , cast(poh.orderdate as datetime) as order_date
        , cast(poh.shipdate as datetime) as ship_date
        , cast(poh.subtotal as float64) as sub_total
        , cast(poh.taxamt as float64) as tax_amount
        , cast(poh.freight as float64) as freight
        , cast(substr(poh.modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'purchaseorderheader') }} poh
)

select
      purchase_order_id
    , revision_number
    , status
    , employee_id
    , vendor_id
    , ship_method_id
    , order_date
    , ship_date
    , sub_total
    , tax_amount
    , freight
    , last_modified_date
from
    stg_purchase_order_header
order by
    purchase_order_id
