{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the sales order header data, ensuring data consistency and validating key constraints and values.'
) }}

with stg_sales_order_header as (
    select
        cast(salesorderid as int64) as sales_order_id
        , cast(revisionnumber as int64) as revision_number
        , cast(orderdate as datetime) as order_date
        , cast(duedate as datetime) as due_date
        , cast(shipdate as datetime) as ship_date
        , cast(status as int64) as status
        , case
            when onlineorderflag = 't' then true
            when onlineorderflag = 'f' then false
            else null
          end as online_order_flag
        , cast(purchaseordernumber as string) as purchase_order_number
        , cast(accountnumber as string) as account_number
        , cast(customerid as int64) as customer_id
        , cast(salespersonid as int64) as sales_person_id
        , cast(territoryid as int64) as sales_territory_id
        , cast(billtoaddressid as int64) as bill_to_address_id
        , cast(shiptoaddressid as int64) as ship_to_address_id
        , cast(shipmethodid as int64) as ship_method_id
        , cast(creditcardid as int64) as credit_card_id
        , cast(creditcardapprovalcode as string) as credit_card_approval_code
        , cast(currencyrateid as int64) as currency_rate_id
        , cast(subtotal as float64) as sub_total
        , cast(taxamt as float64) as tax_amount
        , cast(freight as float64) as freight
        , cast(totaldue as float64) as total_due
        , cast(comment as int64) as comment
        , cast(rowguid as string) as row_guid
        , cast(modifieddate as datetime) as last_modified_date
    from {{ source('stg_adventure_works', 'salesorderheader') }}
)

select
    sales_order_id
    , revision_number
    , order_date
    , due_date
    , ship_date
    , status
    , online_order_flag
    , purchase_order_number
    , account_number
    , customer_id
    , sales_person_id
    , sales_territory_id
    , bill_to_address_id
    , ship_to_address_id
    , ship_method_id
    , credit_card_id
    , credit_card_approval_code
    , currency_rate_id
    , sub_total
    , tax_amount
    , freight
    , total_due
    , comment
    , row_guid
    , last_modified_date
from stg_sales_order_header
order by sales_order_id;