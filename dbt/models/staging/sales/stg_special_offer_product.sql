{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the special offer product data, ensuring data consistency and validating key constraints, unique identifiers, and relationships.'
) }}

with stg_special_offer_product as (
    select
        cast(specialofferid as int64) as special_offer_id
        , cast(productid as int64) as product_id
        , cast(rowguid as string) as row_guid
        , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'specialofferproduct') }}
)

select
    special_offer_id
    , product_id
    , row_guid
    , last_modified_date
from
    stg_special_offer_product
order by
    special_offer_id
    , product_id;
