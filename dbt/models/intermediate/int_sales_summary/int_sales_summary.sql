{{ config(
    materialized='incremental',
    unique_key='order_item_sk',
    description='This model aggregates sales order details at the product level, including calculated freight, tax rates, and sales reasons. It generates a unique surrogate key for each order item to ensure data integrity and supports incremental updates for efficient processing.'
) }}

with calculated_freight as (
    select
        soh.sales_order_id
        , sod.product_id
        , soh.customer_id
        , soh.sales_person_id
        , soh.sales_territory_id
        , sr.sales_reason_id
        , soh.currency_rate_id
        , sc.store_id
        , soh.order_date
        , soh.ship_date
        , soh.total_due
        , soh.sub_total
        , soh.tax_amount
        , soh.freight as total_order_freight
        , sm.ship_base as total_ship_base
        , sm.ship_rate
        , sod.order_qty
        , sod.unit_price
        , sod.unit_price_discount
        , sod.carrier_tracking_number
        , sod.special_offer_id
        , sr.name as sales_reason_name
        , sr.reason_type as sales_reason_type
        , cr.from_currency_code
        , cr.to_currency_code
        , cr.average_rate
        , cc.card_type as credit_card_type
        , soh.revision_number
        , soh.due_date
        , soh.status
        , soh.online_order_flag
        , soh.purchase_order_number
        , soh.ship_method_id
        , sm.ship_method_name
        , round(case
            when trim(um.unit_measure_code) = 'KG' then p.weight * 2.20462
            when trim(um.unit_measure_code) = 'G' then p.weight * 0.00220462
            when trim(um.unit_measure_code) = 'OZ' then p.weight * 0.0625
            when trim(um.unit_measure_code) = 'LB' then p.weight
            else null
          end, 2) as unit_weight_lbs
        , trim(um.unit_measure_code) as unit_measure_code
        , round(sod.order_qty * sod.unit_price * (1 - sod.unit_price_discount), 2) as item_total
    from {{ ref('stg_sales_order_header') }} soh
    left join {{ ref('stg_sales_order_detail') }} sod
        on soh.sales_order_id = sod.sales_order_id
    left join {{ ref('stg_currency_rate') }} cr
        on soh.currency_rate_id = cr.currency_rate_id
    left join {{ ref('stg_sales_order_header_sales_reason') }} soh_sr
        on soh.sales_order_id = soh_sr.sales_order_id
    left join {{ ref('stg_sales_reason') }} sr
        on soh_sr.sales_reason_id = sr.sales_reason_id
    left join {{ ref('stg_customer') }} c
        on soh.customer_id = c.customer_id
    left join {{ ref('stg_store') }} sc
        on c.store_id = sc.business_entity_id
    left join {{ ref('stg_credit_card') }} cc
        on soh.credit_card_id = cc.credit_card_id
    left join {{ ref('stg_ship_method') }} sm
        on soh.ship_method_id = sm.ship_method_id
    left join {{ ref('stg_product') }} p
        on sod.product_id = p.product_id
    left join {{ ref('stg_unit_measure') }} um
        on p.weight_unit_measure_id = um.unit_measure_id
),

freight_with_weight as (
    select
        cf.*
        , round((cf.unit_weight_lbs * cf.ship_rate + cf.total_ship_base * (cf.item_total / sum(cf.item_total) over (partition by cf.sales_order_id))), 2) as calculated_freight
    from calculated_freight cf
    where cf.unit_weight_lbs is not null
),

freight_summary as (
    select
        cf.sales_order_id
        , sum(fww.calculated_freight) as total_calculated_freight
        , countif(cf.unit_weight_lbs is null) as count_no_weight
    from calculated_freight cf
    left join freight_with_weight fww
        on cf.sales_order_id = fww.sales_order_id and cf.product_id = fww.product_id
    group by cf.sales_order_id
),

freight_distribution as (
    select
        cf.*
        , round(case
            when cf.unit_weight_lbs is not null then fww.calculated_freight
            else (cf.total_order_freight - fs.total_calculated_freight) / fs.count_no_weight
          end, 2) as unit_freight_price
    from calculated_freight cf
    left join freight_with_weight fww
        on cf.sales_order_id = fww.sales_order_id and cf.product_id = fww.product_id
    left join freight_summary fs
        on cf.sales_order_id = fs.sales_order_id
),

tax_rates as (
    select
        distinct sc.store_id
        , a.state_province_id
        , tr.tax_rate
        , tr.tax_type
    from {{ ref('stg_store') }} sc
    left join {{ ref('stg_customer') }} c
        on sc.business_entity_id = c.store_id
    left join {{ ref('stg_business_entity_address') }} bea
        on sc.business_entity_id = bea.business_entity_id
    left join {{ ref('stg_address') }} a
        on bea.address_id = a.address_id
    left join {{ ref('stg_state_province') }} sp
        on a.state_province_id = sp.state_province_id
    left join {{ ref('stg_sales_tax_rate') }} tr
        on sp.state_province_id = tr.state_province_id
),

final_tax_rates as (
    select
        fd.*
        , round(coalesce(
            tr2.tax_rate,
            tr3.tax_rate,
            fd.tax_amount / fd.sub_total * 100
        ), 2) as item_tax_rate
    from freight_distribution fd
    left join tax_rates tr2
        on fd.store_id = tr2.store_id and tr2.tax_type = 2
    left join tax_rates tr3
        on fd.store_id = tr3.store_id and tr3.tax_type = 3
)

select distinct
    ftr.sales_order_id
    , ftr.product_id
    , ftr.customer_id
    , ftr.sales_person_id
    , ftr.sales_territory_id
    , STRING_AGG(cast(ftr.sales_reason_id as string), ', ') as sales_reason_ids
    , STRING_AGG(ftr.sales_reason_name, ', ') as sales_reason_names
    , STRING_AGG(ftr.sales_reason_type, ', ') as sales_reason_types
    , ftr.currency_rate_id
    , ftr.store_id
    , ftr.order_date
    , ftr.ship_date
    , ftr.unit_freight_price
    , ftr.item_tax_rate
    , ftr.from_currency_code
    , ftr.to_currency_code
    , ftr.average_rate
    , ftr.credit_card_type
    , ftr.order_qty
    , ftr.unit_price
    , ftr.unit_price_discount
    , ftr.carrier_tracking_number
    , ftr.special_offer_id
    , ftr.revision_number
    , ftr.due_date
    , ftr.status
    , ftr.online_order_flag
    , ftr.purchase_order_number
    , ftr.ship_method_id
    , ftr.ship_method_name
    , ftr.unit_weight_lbs as unit_weight
    , ftr.unit_measure_code
    , ROW_NUMBER() OVER (ORDER BY ftr.sales_order_id, ftr.product_id, ftr.customer_id, ftr.sales_territory_id) as order_item_sk
from final_tax_rates ftr
group by
    ftr.sales_order_id
    , ftr.product_id
    , ftr.customer_id
    , ftr.sales_person_id
    , ftr.sales_territory_id
    , ftr.currency_rate_id
    , ftr.store_id
    , ftr.order_date
    , ftr.ship_date
    , ftr.unit_freight_price
    , ftr.item_tax_rate
    , ftr.from_currency_code
    , ftr.to_currency_code
    , ftr.average_rate
    , ftr.credit_card_type
    , ftr.order_qty
    , ftr.unit_price
    , ftr.unit_price_discount
    , ftr.carrier_tracking_number
    , ftr.special_offer_id
    , ftr.revision_number
    , ftr.due_date
    , ftr.status
    , ftr.online_order_flag
    , ftr.purchase_order_number
    , ftr.ship_method_id
    , ftr.ship_method_name
    , ftr.unit_weight_lbs
    , ftr.unit_measure_code
order by ftr.sales_order_id, ftr.product_id
