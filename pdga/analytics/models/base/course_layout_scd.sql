{{ config(materialized = 'ephemeral') }}

select distinct
    md5(round_course || round_layout) as course_layout_id,
    round_course,
    round_layout,
    layout_holes,
    layout_par,
    layout_distance
from {{ source('pdga_stg', 'event_details') }}