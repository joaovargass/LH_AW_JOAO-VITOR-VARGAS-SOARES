{{ config(
    materialized = 'table',
    schema = 'dev_adventure_works'
) }}

with sales_summary as (
    select
        md5(concat(
            cast(sales_order_id as string),
            cast(sales_order_detail_id as string),
            cast(product_id as string)
        )) as sales_order_item_sk,
        sales_order_id
        , sales_order_detail_id
        , product_id
        , revision_number
        , order_date
        , due_date
        , ship_date
        , status_id
        , status_name
        , online_order_flag
        , purchase_order_number
        , account_number
        , customer_id
        , store_id
        , sales_person_id
        , sales_territory_id
        , payment_method
        , tax_rate
        , avg_unit_freight
        , comment
        , sales_reason_names
        , sales_reason_types
        , line_total
        , ship_method_name
        , ship_base
        , ship_rate
        , from_currency_code
        , to_currency_code
        , average_rate
        , order_qty
        , unit_price
        , unit_price_discount
        , special_offer_id
        , carrier_tracking_number
    from {{ ref('int_sales_summary') }}
)

select *
from sales_summary
order by sales_order_item_sk
