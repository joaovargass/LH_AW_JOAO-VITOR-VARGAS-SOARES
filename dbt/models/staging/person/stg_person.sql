{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the person data, formats names with capitalized first letters, modifies title values, replaces [NULL] with null.'
) }}

with stg_person as (
    select
        p.BusinessEntityID as business_entity_id
        , upper(p.persontype) as person_type
        , initcap(lower(p.FirstName)) as first_name
        , initcap(lower(p.MiddleName)) as middle_name
        , initcap(lower(p.LastName)) as last_name
        , case
            when p.Title like '%Sra%' then 'Mrs.'
            when p.Title like '%Sr%' then 'Mr.'
            else p.Title
          end as name_title
        , p.suffix as name_suffix
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
    business_entity_id
    , first_name
    , middle_name
    , last_name
    , name_title
    , email_promotion
    , name_style
    , person_type
    , name_suffix
    , additional_contact_info
    , demographics
    , row_guid
    , last_modified_date
from
    stg_person;
