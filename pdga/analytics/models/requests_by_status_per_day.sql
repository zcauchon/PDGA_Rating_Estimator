--probably make this event_request_status_by_day
--use daily partition keya and delete/insert using key
{{
  config(
    materialized = 'incremental',
    unique_key = ['scrape_date', 'status'],
    on_schema_change = 'sync_all_columns'
  )
}}

select 
    scrape_date,
    status,
    count(event_id) as status_count
from {{ ref('event_requests') }} 
group by 
    scrape_date,
    status