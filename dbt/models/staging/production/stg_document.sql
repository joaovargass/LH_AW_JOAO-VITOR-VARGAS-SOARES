{{ config(
  materialized='view',
  schema='stg_adventure_works',
  description='This model standardizes the document data, ensuring data consistency and validating key constraints.'
) }}

with stg_document as (
    select
          cast(documentnode as string) as document_node
        , cast(title as string) as title
        , cast(owner as int64) as owner
        , case
              when folderflag = 't' then true
              when folderflag = 'f' then false
              else null
          end as folder_flag
        , cast(filename as string) as file_name
        , cast(fileextension as string) as file_extension
        , cast(revision as string) as revision
        , cast(changenumber as int64) as change_number
        , cast(status as int64) as status
        , cast(documentsummary as string) as document_summary
        , cast(document as bytes) as document
        , cast(rowguid as string) as row_guid
        , cast(substr(modifieddate, 1, 19) as datetime) as last_modified_date
    from
        {{ source('stg_adventure_works', 'document') }}
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
    document_node;
