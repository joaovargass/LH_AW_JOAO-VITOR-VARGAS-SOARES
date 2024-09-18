{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the special offer product data, ensuring data consistency and validating key constraints, unique identifiers, and relationships.'
) }}

with stg_special_offer_product as (
    select
        cast(specialofferid as int64) as special_offer_id
        , cast(productid as int64) as product_id
    from
        {{ source('stg_adventure_works', 'specialofferproduct') }}
)

select
    *
from
    stg_special_offer_product
order by
    special_offer_id
    , product_id
