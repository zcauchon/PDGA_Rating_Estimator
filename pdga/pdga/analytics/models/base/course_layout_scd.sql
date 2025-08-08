{{ config(materialized = 'ephemeral') }}

select distinct
    md5(round_course || round_layout) as course_layout_id,
    round_course,
    round_layout,
    layout_holes,
    layout_par,
    layout_distance
from {{ source('pdga', 'event_details') }}
{% if is_incremental() %}
  where processing_date between '{{ var('start_date') }}' and '{{ var('end_date') }}'
{% endif %}