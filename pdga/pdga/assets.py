import os
import json
from time import sleep
import pandas as pd
from dotenv import load_dotenv
from requests import get
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text, URL
from sqlalchemy.types import Integer

from project import dbt_project
from constants import request_status
from orchestration.partitions import daily_partitions
from parser.extract_event_info import event_info_extractor

from dagster import asset, AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource

@asset(
    group_name="web",
    compute_kind="python",
    partitions_def=daily_partitions,
)
def event_requests(context: AssetExecutionContext) -> None:
    """
        Find recently update events using pdga tour search
    """
    load_dotenv()
    url_obj = URL.create(
        "postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASS"),
        host=os.getenv("POSTGRES_URL"),
        port=os.getenv("POSTGRES_PORT"),
        database="pdga"
    )
    context.log.info(f"Connecting to {url_obj}")
    engine = create_engine(url_obj)
    
    proxy = {
        "http": os.getenv("PDGA_PROXY"),
        "https": os.getenv("PDGA_PROXY")
    }

    target_date = context.partition_key
    url_base = 'https://www.pdga.com'
    search_url = f'/tour/search?date_filter[min][date]={target_date}&date_filter[max][date]={target_date}&Country[]=United+States&Tier[]=A&Tier[]=A%2FB&Tier[]=A%2FC&Tier[]=B&Tier[]=B%2FA&Tier[]=B%2FC&Tier[]=C&Tier[]=C%2FA&Tier[]=C%2FB'
    results = []

    while True:
        # make request to find recent events
        page = get(f"{url_base}{search_url}", proxies=proxy)
        soup = BeautifulSoup(page.content, "html.parser")
        # parse results 
        for event in soup.find_all('td', attrs={'class':'views-field-OfficialName'}):
            event_url = f'{url_base}{event.a["href"]}'
            # add event to results
            results.append([event_url.split('/')[-1], target_date, 1])
        # check if there are paged results
        next_page = soup.find("li", attrs={"class":"pager-next"})
        if next_page is None:
            # no more results, break out of loop
            break
        print('More results available...')
        #update url to next page and make another request
        search_url = next_page.a["href"]
    
    # once everything has been identified
    # create df of new events and save to table
    df = pd.DataFrame(columns=["event_id", "event_date", "status"], data=results)
    df.drop_duplicates(inplace=True)
    with engine.begin() as con:
        nrows = 0
        if len(df) > 0:
            # clear any request already made for this date that isnt complete
            con.execute(text(f"delete from pdga.event_requests where event_date = '{target_date}' and status != {request_status.complete}"))
            nrows = df.to_sql('event_requests', schema="pdga", if_exists="append", con=con, index=False, dtype={"event_id": Integer})
    print(f'Loaded {nrows} rows for processing')

@asset(
    deps=[event_requests],
    group_name="web",
    compute_kind="python",
    partitions_def=daily_partitions,
)
def event_details(context: AssetExecutionContext) -> pd.DataFrame:
    """
        Get event details for identified events
    """
    load_dotenv()
    url_obj = URL.create(
        "postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASS"),
        host=os.getenv("POSTGRES_URL"),
        port=os.getenv("POSTGRES_PORT"),
        database="pdga"
    )
    engine = create_engine(url_obj)

    target_date = context.partition_key

    with engine.connect() as con:
        _sql = f"""select distinct event_id 
                from pdga.event_requests s
                where event_date = '{target_date}'
                union
                select event_id
                from pdga.event_requests
                where status = 2
                and retry_date = '{target_date}'
                except
                select event_id 
                from pdga.event_requests
                where status = 4;"""
        pending_events = con.execute(text(_sql))
        con.commit()
        results = pd.DataFrame()
        for event in pending_events:
            status, event_info = event_info_extractor(event[0], target_date)
            update_clause = f"set status = {status}"
            if status == request_status.incomplete:
                update_clause = update_clause + ", retry_date = current_date+5"
            _sql = f"update pdga.event_requests {update_clause} where event_id = {event[0]}"
            con.execute(text(_sql))
            con.commit()
            if event_info is not None:
                results = pd.concat([results, event_info])
            sleep(2)
        # anything left in a 1 status means we aleady had data for the event, drop the pending request
        con.execute(text(f"delete from pdga.event_requests where status = {request_status.pending} and event_date = '{target_date}'"))
        if len(results) > 0:
            print(f"Writing {len(results)} new events to event_details for {target_date}")
            results.to_sql("event_details", con=con, schema="pdga", index=False, if_exists="append")
            con.commit()
    return results


### DBT ###
@dbt_assets(
    manifest=dbt_project.manifest_path,
    partitions_def=daily_partitions,
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    start, end = context.partition_key_range
    dbt_vars = {"start_date": start, "end_date": end}
    dbt_build_args = ["build", "--vars", json.dumps(dbt_vars)]
    yield from dbt.cli(dbt_build_args, context=context).stream()
    