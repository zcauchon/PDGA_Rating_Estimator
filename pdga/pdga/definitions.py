from dagster import Definitions, load_assets_from_modules

import assets
from resources import dbt_resource
from orchestration.daily_data_load import daily_schedule

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": dbt_resource
    },
    schedules=[daily_schedule]
)
