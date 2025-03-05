
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
{% if is_incremental() %}
  where scrape_date > (select max(scrape_date) from {{ this }})
{% endif %}
group by 
    scrape_date,
    status