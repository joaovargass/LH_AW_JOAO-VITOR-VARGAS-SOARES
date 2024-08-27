{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the sales tax rate data, ensuring data consistency and validating key constraints and values.'
) }}

with stg_sales_tax_rate as (
    select
        cast(salestaxrateid as int64) as sales_tax_rate_id
        , cast(stateprovinceid as int64) as state_province_id
        , cast(taxtype as int64) as tax_type
        , cast(taxrate as float64) as tax_rate
        , cast(name as string) as name
        , cast(rowguid as string) as row_guid
        , cast(modifieddate as datetime) as last_modified_date
    from {{ source('stg_adventure_works', 'salestaxrate') }}
)

select
    sales_tax_rate_id
    , state_province_id
    , tax_type
    , tax_rate
    , name
    , row_guid
    , last_modified_date
from stg_sales_tax_rate
order by sales_tax_rate_id;
