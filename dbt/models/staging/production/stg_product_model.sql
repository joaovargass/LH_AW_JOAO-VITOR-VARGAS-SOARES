{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the product model data, ensuring data consistency and validating key constraints.'
) }}

with stg_product_model as (
    select
        cast(pm.productmodelid as int64) as product_model_id
        , cast(pm.name as string) as product_model_name
        , cast(pm.catalogdescription as string) as catalog_description
        , cast(pm.instructions as string) as instructions
        , cast(pm.rowguid as string) as row_guid
        , cast(substr(pm.modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'productmodel') }} pm
)

select
    product_model_id
    , product_model_name
    , catalog_description
    , instructions
    , row_guid
    , last_modified_date
from
    stg_product_model
order by
    product_model_id
