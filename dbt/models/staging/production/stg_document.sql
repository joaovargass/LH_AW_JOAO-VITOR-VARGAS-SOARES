{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the document data, ensuring data consistency and validating key constraints.'
) }}

with stg_document as (
    select
          cast(d.documentnode as string) as document_node
        , cast(d.title as string) as title
        , cast(d.owner as int64) as owner
        , case
              when d.folderflag = 't' then true
              when d.folderflag = 'f' then false
              else null
          end as folder_flag
        , cast(d.filename as string) as file_name
        , cast(d.fileextension as string) as file_extension
        , cast(d.revision as string) as revision
        , cast(d.changenumber as int64) as change_number
        , cast(d.status as int64) as status
        , cast(d.documentsummary as string) as document_summary
        , cast(d.document as bytes) as document
        , cast(d.rowguid as string) as row_guid
        , cast(substr(d.modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'document') }} as d
)

select
      document_node
    , title
    , owner
    , folder_flag
    , file_name
    , file_extension
    , revision
    , change_number
    , status
    , document_summary
    , document
    , row_guid
    , last_modified_date
from
    stg_document
order by
    document_node
