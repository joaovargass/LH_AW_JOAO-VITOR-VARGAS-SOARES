{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the sales territory history data, ensuring data consistency and validating key constraints, date integrity, and unique identifiers.'
) }}

with ranked_sales_territory_history as (
    select
        *,
        row_number() over (order by businessentityid, territoryid, startdate) as sales_territory_history_id
    from
        {{ source('stg_adventure_works', 'salesterritoryhistory') }}
)

select
    sales_territory_history_id,
    cast(businessentityid as int64) as business_entity_id,
    cast(territoryid as int64) as sales_territory_id,
    cast(startdate as datetime) as start_date,
    cast(enddate as datetime) as end_date,
    cast(rowguid as string) as row_guid,
    cast(modifieddate as datetime) as last_modified_date
from
    ranked_sales_territory_history
order by
    sales_territory_history_id
