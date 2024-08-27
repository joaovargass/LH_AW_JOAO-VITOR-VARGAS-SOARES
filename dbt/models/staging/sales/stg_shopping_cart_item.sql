{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the shopping cart item data, ensuring data consistency and validating key constraints and values.'
) }}

with stg_shopping_cart_item as (
    select
        cast(shoppingcartitemid as int64) as shopping_cart_item_id
        , cast(shoppingcartid as string) as shopping_cart_id
        , cast(quantity as int64) as quantity
        , cast(productid as int64) as product_id
        , cast(datecreated as datetime) as date_created
        , cast(modifieddate as datetime) as last_modified_date
    from {{ source('stg_adventure_works', 'shoppingcartitem') }}
)

select
    shopping_cart_item_id
    , shopping_cart_id
    , quantity
    , product_id
    , date_created
    , last_modified_date
from stg_shopping_cart_item
order by shopping_cart_item_id;
