{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the sales reason data, ensuring data consistency and validating key constraints, unique identifiers, and relationships.'
) }}

with stg_sales_reason as (
    select
        cast(salesreasonid as int64) as sales_reason_id
        , cast(name as string) as name
        , cast(reasontype as string) as reason_type
        , cast(modifieddate as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'salesreason') }}
)

select
    sales_reason_id
    , name
    , reason_type
    , last_modified_date
from
    stg_sales_reason
order by
    sales_reason_id;
