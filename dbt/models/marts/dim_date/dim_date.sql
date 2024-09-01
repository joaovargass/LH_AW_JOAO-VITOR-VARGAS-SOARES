{{ config(
    materialized = 'table'
    , schema = 'dev_adventure_works'
) }}

with date_bounds as (
    select
          min(coalesce(due_date, ship_date, order_date)) as min_date
        , max(coalesce(due_date, ship_date, order_date)) as max_date
    from {{ ref('int_sales_summary') }}
    union all
    select
          min(start_date) as min_date
        , max(end_date) as max_date
    from {{ ref('int_special_offer_info') }}
    union all
    select
          min(sell_start_date) as min_date
        , max(sell_end_date) as max_date
    from {{ ref('int_product_info') }}
)

, date_extents as (
    select
          min(min_date) as min_date
        , max(max_date) as max_date
    from date_bounds
)

, date_range as (
    select
          date_add(min_date, interval n day) as full_date
    from date_extents
    cross join unnest(generate_array(
          0
        , date_diff(
              date_add(max_date, interval 2 year)
            , min_date
            , day
          )
    )) as n
)

select
      full_date as date_id
    , full_date
    , extract(year from full_date) as year
    , extract(quarter from full_date) as quarter
    , extract(month from full_date) as month
    , extract(day from full_date) as day
    , extract(week from full_date) as week
    , case
        when extract(dayofweek from full_date) in (1, 7)
        then true
        else false
      end as is_weekend
    , extract(year from full_date)
      + case
          when extract(month from full_date) > 6
          then 1
          else 0
        end as fiscal_year
    , cast(ceil(extract(month from full_date) / 3.0) as int) as fiscal_quarter
from date_range
