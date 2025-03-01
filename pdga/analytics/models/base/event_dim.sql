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
    event_date,
    event_city,
    event_state,
    event_country,
    event_director,
    event_type,
    event_purse
from {{ source('pdga_stg', 'event_details') }}