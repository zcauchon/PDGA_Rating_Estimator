{{
  config(
    materialized = 'incremental',
    unique_key = 'event_id',
    on_schema_change = 'sync_all_columns'
  )
}}

select distinct
    event_id,
    event_name,
    split_part(event_date, ' ', -1)::date::text as event_date,
    event_city,
    event_state,
    event_country,
    event_director,
    event_type,
    event_purse
from {{ source('pdga', 'event_details') }}
{% if is_incremental() %}
  where processing_date between '{{ var('start_date') }}' and '{{ var('end_date') }}'
{% endif %}