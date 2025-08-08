--# of players, avg prize, avg rating, # < par, # > par
{{
  config(
    materialized = 'incremental',
    unique_key = ['event_id', 'player_pdga','round_number'],
    check_cols = ['event_id', 'player_pdga','round_number'],
    on_schema_change = 'sync_all_columns'
  )
}}

select distinct
    event_id,
    player_pdga,
    md5(round_course || round_layout) as course_layout_id,
    event_division,
    round_number,
    player_round_score,
    player_round_rating
from {{ source('pdga', 'event_details') }}
where player_pdga != ''
{% if is_incremental() %}
  and processing_date between '{{ var('start_date') }}' and '{{ var('end_date') }}'
{% endif %}