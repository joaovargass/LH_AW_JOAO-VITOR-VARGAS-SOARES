{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the sales order header sales reason data, ensuring data consistency and validating key constraints.'
) }}

with stg_sales_order_header_sales_reason as (
    select
        cast(salesorderid as int64) as sales_order_id
        , cast(salesreasonid as int64) as sales_reason_id
    from {{ source('stg_adventure_works', 'salesorderheadersalesreason') }}
)
select
    *
from stg_sales_order_header_sales_reason
order by sales_order_id, sales_reason_id
