{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the sales order detail data, ensuring data consistency and validating key constraints and values.'
) }}

with stg_sales_order_detail as (
    select
        cast(salesorderid as int64) as sales_order_id
        , cast(salesorderdetailid as int64) as sales_order_detail_id
        , cast(productid as int64) as product_id
        , cast(specialofferid as int64) as special_offer_id
        , carriertrackingnumber as carrier_tracking_number
        , cast(orderqty as int64) as order_qty
        , cast(unitprice as float64) as unit_price
        , cast(unitpricediscount as float64) as unit_price_discount
        , cast(rowguid as string) as row_guid
        , cast(modifieddate as datetime) as last_modified_date
    from {{ source('stg_adventure_works', 'salesorderdetail') }}
)

select
    sales_order_id
    , sales_order_detail_id
    , product_id
    , special_offer_id
    , carrier_tracking_number
    , order_qty
    , unit_price
    , unit_price_discount
    , row_guid
    , last_modified_date
from stg_sales_order_detail
order by sales_order_detail_id;
