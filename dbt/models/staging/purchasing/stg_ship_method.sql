{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the ship method data, ensuring data consistency and enforcing the foreign key constraint on shipmethodid, which is also the primary key.'
) }}

with stg_ship_method as (
    select
          cast(sm.shipmethodid as int64) as ship_method_id
        , sm.name as ship_method_name
        , cast(sm.shipbase as float64) as ship_base
        , cast(sm.shiprate as float64) as ship_rate
    from
        {{ source('stg_adventure_works', 'shipmethod') }} sm
)

select
      ship_method_id
    , ship_method_name
    , ship_base
    , ship_rate
from
    stg_ship_method
order by
    ship_method_id
