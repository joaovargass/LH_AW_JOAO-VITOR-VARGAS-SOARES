{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='Este modelo padroniza os dados de associação entre produtos e fotos de produtos, garantindo consistência nos dados e validando as chaves primárias e campos datetime.'
) }}

with stg_product_product_photo as (
    select
          cast(productid as int64) as product_id
        , cast(productphotoid as int64) as product_photo_id
        , case
            when primary = 't' then true
            when primary = 'f' then false
            else null
          end as is_primary
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'productproductphoto') }}
)

select
      product_id
    , product_photo_id
    , is_primary
    , last_modified_date
from
    stg_product_product_photo
order by
    product_id,
    product_photo_id;
