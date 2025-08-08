
{{
  config(
    materialized = 'incremental',
    unique_key = ['event_date', 'status'],
    on_schema_change = 'sync_all_columns'
  )
}}

select 
    event_date,
    status,
    count(event_id) as status_count
from {{ source('pdga', 'event_requests') }} 
{% if is_incremental() %}
  where event_date between '{{ var('start_date') }}' and '{{ var('end_date') }}'
{% endif %}
group by 
    event_date,
    status