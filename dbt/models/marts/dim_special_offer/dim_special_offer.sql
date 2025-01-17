{{ config(
    materialized = 'table',
    schema = 'dev_adventure_works'
) }}

with dim_special_offer as (
    select
        special_offer_id,
        offer_description,
        discount_pct,
        offer_type,
        offer_category,
        start_date,
        end_date,
        min_qty,
        max_qty
    from {{ ref('int_special_offer_info') }}
)

select *
from dim_special_offer
order by special_offer_id
