{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the special offer data, ensuring data consistency and validating key constraints, unique identifiers, and relationships.'
) }}

with stg_special_offer as (
    select
        cast(specialofferid as int64) as special_offer_id
        , cast(description as string) as offer_description
        , cast(discountpct as numeric) as discount_pct
        , cast(type as string) as offer_type
        , cast(category as string) as offer_category
        , cast(startdate as datetime) as start_date
        , cast(enddate as datetime) as end_date
        , cast(minqty as int64) as min_qty
        , cast(maxqty as int64) as max_qty
    from
        {{ source('stg_adventure_works', 'specialoffer') }}
)

select
    *
from
    stg_special_offer
order by
    special_offer_id
