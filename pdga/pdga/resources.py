from dagster_snowflake import SnowflakeResource
from dagster import EnvVar

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