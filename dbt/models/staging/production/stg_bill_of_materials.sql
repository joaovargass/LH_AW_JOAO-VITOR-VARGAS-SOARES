{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the bill of materials data, ensuring data consistency and validating key constraints, date integrity, and quantity values.'
) }}

with stg_bill_of_materials as (
    select
          cast(bom.billofmaterialsid as int64) as bill_of_materials_id
        , cast(bom.productassemblyid as int64) as product_assembly_id
        , cast(bom.componentid as int64) as component_id
        , cast(bom.startdate as datetime) as start_date
        , cast(bom.enddate as datetime) as end_date
        , um.unit_measure_id as unit_measure_id
        , cast(bom.bomlevel as int64) as bom_level
        , cast(bom.perassemblyqty as float64) as per_assembly_qty
        , cast(bom.modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'billofmaterials') }} bom
    left join
        {{ ref('stg_unit_measure') }} um
    on
        bom.unitmeasurecode = um.unit_measure_code
)

select
      bill_of_materials_id
    , product_assembly_id
    , component_id
    , start_date
    , end_date
    , unit_measure_id
    , bom_level
    , per_assembly_qty
    , last_modified_date
from
    stg_bill_of_materials
order by
    bill_of_materials_id
