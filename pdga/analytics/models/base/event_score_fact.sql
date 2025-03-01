--# of players, avg prize, avg rating, # < par, # > par

select distinct
    event_id,
    player_pdga,
    md5(round_course || round_layout) as course_layout_id,
    event_division,
    round_number,
    player_round_score,
    player_round_rating
from {{ source('pdga_stg', 'event_details') }}