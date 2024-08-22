{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the person data, formats names with capitalized first letters, modifies title values, replaces [NULL] with null, and adds a unique person_id.'
) }}

with stg_person as (
    select
          cast(row_number() over (order by p.BusinessEntityID) as int64) as person_id
        , p.BusinessEntityID as business_entity_id
        , initcap(lower(p.FirstName)) as first_name
        , initcap(lower(p.MiddleName)) as middle_name
        , initcap(lower(p.LastName)) as last_name
        , case
            when p.Title like '%Sra%' then 'Mrs'
            when p.Title like '%Sr%' then 'Mr'
            else p.Title
          end as title
        , cast(p.EmailPromotion as int64) as email_promotion
        , cast(p.NameStyle as boolean) as name_style
        , case
            when p.AdditionalContactInfo = '[NULL]' then null
            else p.AdditionalContactInfo
          end as additional_contact_info
        , case
            when p.Demographics = '[NULL]' then null
            else p.Demographics
          end as demographics
        , cast(p.rowguid as string) as row_guid
        , cast(substr(p.ModifiedDate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'person') }} p
)

select
      person_id
    , business_entity_id
    , first_name
    , middle_name
    , last_name
    , title
    , email_promotion
    , name_style
    , person_type
    , suffix
    , additional_contact_info
    , demographics
    , row_guid
    , last_modified_date
from
    stg_person;
