import pandas as pd
from datetime import date, timedelta
from requests import get
from bs4 import BeautifulSoup
from snowflake.connector.pandas_tools import write_pandas
from .parser.extract_event_info import event_info_extractor

from .project import dbt_project

from dagster import asset, AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource
from dagster_snowflake import SnowflakeResource

@asset(
    group_name="web",
    compute_kind="python"
)
def event_requests_stg(snowflake_pdga_stg: SnowflakeResource) -> None:
    """
        Find recently update events using pdga tour search
    """
    d = date.today() - timedelta(days=1)
    min_date = d.strftime('%Y-%m-%d')
    url_base = 'https://www.pdga.com'
    search_url = f'/tour/search?date_filter[min][date]={min_date}&State[]=PA&Tier[]=A&Tier[]=B&Tier[]=C'
    results = []

    while True:
        # make request to find recent events
        page = get(f"{url_base}{search_url}")
        soup = BeautifulSoup(page.content, "html.parser")
        # parse results 
        for event in soup.find_all('td', attrs={'class':'views-field-OfficialName'}):
            event_url = f'{url_base}{event.a["href"]}'
            # add event to results
            results.append([event_url.split('/')[-1], date.today(), 1])
        # check if there are paged results
        next_page = soup.find("li", attrs={"class":"pager-next"})
        if next_page is None:
            # no more results, break out of loop
            break
        print('More results available...')
        #update url to next page and make another request
        search_url = next_page.a["href"]
    
    # once everything has been identified
    # create df of new events and save to stg
    df = pd.DataFrame(columns=["EVENT_ID", "SCRAPE_DATE", "STATUS"], data=results)
    df.drop_duplicates(inplace=True)
    with snowflake_pdga_stg.get_connection() as con:
        success, nchunks, nrows, _ = write_pandas(con, df, 'EVENT_REQUESTS', auto_create_table=True, overwrite=True)
        print(f'Loaded {nrows} rows for processing')

@asset(
    deps=[event_requests_stg],
    group_name="web",
    compute_kind="python"
)
def event_details_stg(snowflake_pdga_stg: SnowflakeResource) -> pd.DataFrame:
    """
        Get event details for identified events
    """
    with snowflake_pdga_stg.get_connection() as con:
        _sql = """select distinct event_id 
                from EVENT_REQUESTS s
                where status = 1"""
        pending_events = con.cursor().execute(_sql) # this feels so wrong, can i make this a dbt query?
        results = pd.DataFrame()
        for event in pending_events:
            status, event_info = event_info_extractor(event[0])
            # would probably be better to do this all at once
            con.cursor().execute(f"update EVENT_REQUESTS set status = {status} where event_id = {event[0]}")
            if event_info is not None:
                results = pd.concat([results, event_info])
        con.cursor().execute("truncate pdga_stg.event_details")
        if len(results) > 0:
            print(f"Writing {len(results)} new events to EVENT_DETAILS")
            write_pandas(con, results, "EVENT_DETAILS", auto_create_table=True)
    return results


### DBT ###
@dbt_assets(
    manifest=dbt_project.manifest_path
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    dbt_build_invocation = dbt.cli(["build"], context=context)
    yield from dbt_build_invocation.stream()