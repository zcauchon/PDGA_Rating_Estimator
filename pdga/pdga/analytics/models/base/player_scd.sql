{{ config(materialized = 'ephemeral') }}

select distinct
    player_pdga,
    player_rating
from {{ source('pdga', 'event_details') }}
where player_pdga != ''
{% if is_incremental() %}
  and processing_date between '{{ var('start_date') }}' and '{{ var('end_date') }}'
{% endif %}