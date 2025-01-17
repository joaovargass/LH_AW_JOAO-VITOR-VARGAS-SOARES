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
        , cast(soh.purchaseordernumber as string) as purchase_order_number
        , cast(soh.accountnumber as string) as account_number
        , cast(soh.customerid as int64) as customer_id
        , cast(sp.sales_person_id as int64) as sales_person_id
        , cast(soh.territoryid as int64) as sales_territory_id
        , cast(soh.shipmethodid as int64) as ship_method_id
        , cast(soh.creditcardid as int64) as credit_card_id
        , cast(soh.creditcardapprovalcode as string) as credit_card_approval_code
        , cast(soh.currencyrateid as int64) as currency_rate_id
        , cast(soh.subtotal as float64) as sub_total
        , cast(soh.taxamt as float64) as tax_amount
        , cast(soh.freight as float64) as freight
        , cast(soh.totaldue as float64) as total_due
        , cast(soh.comment as int64) as comment
    from {{ source('stg_adventure_works', 'salesorderheader') }} soh
    left join {{ ref('stg_sales_person') }} as sp
        on soh.salespersonid = sp.business_entity_id
)

select
    *
from stg_sales_order_header
order by sales_order_id
