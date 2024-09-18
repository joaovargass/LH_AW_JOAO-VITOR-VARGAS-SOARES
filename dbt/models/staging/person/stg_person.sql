{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the person data, formats names with capitalized first letters, modifies title values, replaces [NULL] with null.'
) }}

with stg_person as (
    select
        p.BusinessEntityID as business_entity_id
        , upper(p.persontype) as person_type
        , case
            when p.NameStyle = 't' then initcap(lower(p.LastName))
            else initcap(lower(p.FirstName))
        end as first_name
        , initcap(lower(p.MiddleName)) as middle_name
        , case
            when p.NameStyle = 't' then initcap(lower(p.FirstName))
            else initcap(lower(p.LastName))
        end as last_name
        , case
            when p.Title like '%Sra%' then 'Mrs.'
            when p.Title like '%Sr%' then 'Mr.'
            else p.Title
          end as name_title
        , p.suffix as name_suffix
        , cast(p.EmailPromotion as int64) as email_promotion
        , case
            when p.AdditionalContactInfo = '[NULL]' then null
            else p.AdditionalContactInfo
          end as additional_contact_info
        , case
            when p.Demographics = '[NULL]' then null
            else p.Demographics
          end as demographics
    from
        {{ source('stg_adventure_works', 'person') }} p
)

select
    *
from
    stg_person
