from dagster import Definitions, load_assets_from_modules

from . import assets
from .resources import pdga, pdga_stg, dbt_resource
from .orchestration.daily_data_load import daily_schedule

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "snowflake_pdga": pdga,
        "snowflake_pdga_stg": pdga_stg,
        "dbt": dbt_resource
    },
    schedules=[daily_schedule]
)
