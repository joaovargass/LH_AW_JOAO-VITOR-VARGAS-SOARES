{{ config(
    materialized='view',
    schema='dev_adventure_works',
    description='This view aggregates sales order data, including details on customers, salespersons, reasons for purchase, and shipping information. It also performs various transformations such as calculating tax rate, aggregating sales reasons, and determining the payment method.'
) }}

with sales_order_header as (
    select
        sales_order_id
        , revision_number
        , order_date
        , due_date
        , ship_date
        , status as status_id
        , case
            when status = 1 then 'In Process'
            when status = 2 then 'Approved'
            when status = 3 then 'Backordered'
            when status = 4 then 'Rejected'
            when status = 5 then 'Shipped'
            when status = 6 then 'Cancelled'
            else 'Unknown'
          end as status_name
        , online_order_flag
        , purchase_order_number
        , account_number
        , customer_id
        , sales_person_id
        , sales_territory_id
        , ship_method_id
        , currency_rate_id
        , case
            when credit_card_id is not null then 'CREDIT CARD'
            else 'OTHER'
          end as payment_method
        , sub_total
        , tax_amount
        , freight
        , sub_total + tax_amount + freight as total_due
        , tax_amount / sub_total as tax_rate
        , comment
    from {{ ref('stg_sales_order_header') }}
)

, customer_data as (
    select
        customer_id
        , store_id
    from {{ ref('stg_customer') }}
)

, sales_reason_agg as (
    select
        sales_order_id
        , string_agg(name, ', ' order by name) as sales_reason_names
        , string_agg(reason_type, ', ' order by reason_type) as sales_reason_types
    from {{ ref('stg_sales_reason') }} sr
    join {{ ref('stg_sales_order_header_sales_reason') }} srh
      on sr.sales_reason_id = srh.sales_reason_id
    group by sales_order_id
)

, sales_order_detail as (
    select
        sales_order_id
        , sales_order_detail_id
        , product_id
        , order_qty
        , unit_price
        , unit_price_discount
        , special_offer_id
        , carrier_tracking_number
        , sum(line_total) as line_total
    from {{ ref('stg_sales_order_detail') }}
    group by sales_order_id, sales_order_detail_id, product_id, order_qty, unit_price, unit_price_discount, special_offer_id, carrier_tracking_number
)

, shipping_info as (
    select
        ship_method_id
        , ship_method_name
        , ship_base
        , ship_rate
    from {{ ref('stg_ship_method') }}
)

, currency_rate_info as (
    select
        currency_rate_id
        , from_currency_code
        , to_currency_code
        , average_rate
    from {{ ref('stg_currency_rate') }}
)

, int_sales_summary as (
    select
        soh.sales_order_id
        , sod.sales_order_detail_id
        , sod.product_id
        , soh.revision_number
        , soh.order_date
        , soh.due_date
        , soh.ship_date
        , soh.status_id
        , soh.status_name
        , soh.online_order_flag
        , soh.purchase_order_number
        , soh.account_number
        , soh.customer_id
        , c.store_id
        , soh.sales_person_id
        , soh.sales_territory_id
        , soh.payment_method
        , soh.tax_rate
        , soh.freight/sod.order_qty as avg_unit_freight
        , soh.comment
        , soh.sub_total
        , sr.sales_reason_names
        , sr.sales_reason_types
        , sod.order_qty
        , sod.unit_price
        , sod.unit_price_discount
        , sod.special_offer_id
        , sod.carrier_tracking_number
        , sod.line_total
        , si.ship_method_name
        , si.ship_base
        , si.ship_rate
        , cr.from_currency_code
        , cr.to_currency_code
        , cr.average_rate
    from sales_order_header soh
    left join customer_data c
      on soh.customer_id = c.customer_id
    left join sales_reason_agg sr
      on soh.sales_order_id = sr.sales_order_id
    left join sales_order_detail sod
      on soh.sales_order_id = sod.sales_order_id
    left join shipping_info si
      on soh.ship_method_id = si.ship_method_id
    left join currency_rate_info cr
      on soh.currency_rate_id = cr.currency_rate_id
)

select * from int_sales_summary
