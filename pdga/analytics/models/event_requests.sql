-- move requests from stg to main schema for tracking
{{
  config(
    materialized = 'incremental',
    unique_key = ['event_id','scrape_date'],
    on_schema_change = 'sync_all_columns'
  )
}}

with details as (
    -- dont need any info from here, just need the dependency
    select 1 from {{ source('pdga_stg', 'event_details') }} where 1 = 2
)
select * from {{ source('pdga_stg', 'event_requests') }}
where status != 1