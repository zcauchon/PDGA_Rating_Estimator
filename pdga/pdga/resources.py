from dagster_snowflake import SnowflakeResource
from dagster_dbt import DbtCliResource
from dagster import EnvVar

from .project import dbt_project

pdga_stg = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    warehouse="PDGA_WH",
    database="PDGA_DB",
    schema="PDGA_STG"
)

pdga = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    warehouse="PDGA_WH",
    database="PDGA_DB",
    schema="PDGA"
)

dbt_resource = DbtCliResource(
    project_dir=dbt_project
)