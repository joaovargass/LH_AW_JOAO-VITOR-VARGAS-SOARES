{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the job candidate data, ensures that job_candidate_id is the primary key, allows for nullable business_entity_id as a foreign key, and validates the modified date.'
) }}

with stg_job_candidate as (
    select
          cast(jobcandidateid as int64) as job_candidate_id
        , cast(businessentityid as int64) as business_entity_id
        , resume as job_candidate_resume
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'jobcandidate') }}
)

select
      job_candidate_id
    , business_entity_id
    , job_candidate_resume
    , last_modified_date
from
    stg_job_candidate
order by
    job_candidate_id
