with event_counts as (
    select event_id, count(distinct player_pdga) as event_players
    from {{ ref('event_score_fact') }}
    group by event_id
),
events as (
    select event_id, date_trunc('month', date(event_date)) as event_date
    from {{ ref('event_dim') }}
    where event_date::timestamp >= now() + interval '-36 months'
)
select 
    e.event_date, 
    sum(ec.event_players) as player_count
from events e
join event_counts ec on
    e.event_id = ec.event_id
group by e.event_date
order by 1
