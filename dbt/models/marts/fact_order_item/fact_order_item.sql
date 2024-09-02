{{ config(
    materialized = 'table'
    , schema = 'dev_adventure_works'
) }}

with sales_summary as (
    select
        md5(concat(
            cast(order_date as string),
            cast(sales_order_id as string),
            cast(customer_id as string),
            cast(product_id as string)
        )) as sales_order_item_sk
        , sales_order_id
        , product_id
        , customer_id
        , sales_person_id
        , sales_territory_id
        , sales_reason_ids
        , sales_reason_names
        , sales_reason_types
        , currency_rate_id
        , store_id
        , order_date
        , ship_date
        , unit_freight_price
        , item_tax_rate
        , from_currency_code
        , to_currency_code
        , average_rate
        , credit_card_type
        , order_qty
        , unit_price
        , unit_price_discount
        , carrier_tracking_number
        , special_offer_id
        , revision_number
        , due_date
        , status
        , online_order_flag
        , purchase_order_number
        , ship_method_id
        , ship_method_name
        , unit_weight
        , unit_measure_code
    from {{ ref('int_sales_summary') }}
)

select *
from sales_summary
