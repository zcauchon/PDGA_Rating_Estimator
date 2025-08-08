select player_pdga, count(*) as rounds_played
from {{ ref('event_score_fact') }}
group by player_pdga 
order by 2 desc limit 10