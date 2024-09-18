{{ config(
    materialized='view',
    schema='stg_adventure_works',
    description='This model standardizes the sales person quota history data, ensuring data consistency and validating key constraints, unique identifiers, and relationships.'
) }}

with stg_sales_person_quota_history as (
    select
        row_number() over (order by businessentityid, quotadate) as sales_person_quota_history_id
        , cast(businessentityid as int64) as business_entity_id
        , cast(quotadate as datetime) as quota_date
        , cast(salesquota as numeric) as sales_quota
    from
        {{ source('stg_adventure_works', 'salespersonquotahistory') }}
)

select
    *
from
    stg_sales_person_quota_history
order by
    sales_person_quota_history_id
