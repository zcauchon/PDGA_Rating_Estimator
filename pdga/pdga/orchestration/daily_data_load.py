
from dagster import ScheduleDefinition, define_asset_job

daily_refresh_job = define_asset_job(
    "daily_refresh",
    selection=[
        "event_requests_stg",
        "event_details_stg",
        "event_requests",
        "requests_by_status_per_day",
        "player_dim",
        "event_dim",
        "course_layout_dim",
        "event_score_fact"
    ]
)

daily_schedule = ScheduleDefinition(
    job=daily_refresh_job,
    cron_schedule="0 19 * * *" # run everyday at 7pm
)