import pandas as pd
from datetime import date, timedelta
from requests import get
from bs4 import BeautifulSoup
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from .parser.extract_event_info import event_info_extractor

from dagster import Output, asset

@asset
def event_requests() -> None:
    d = date.today() - timedelta(days=15)
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
    with snowflake.connector.connect(
        connection_name="stg"
    ) as con:
        success, nchunks, nrows, _ = write_pandas(con, df, 'EVENT_REQUESTS', auto_create_table=True, overwrite=True)
        print(f'Loaded {nrows} rows for processing')

@asset(
    deps=[event_requests]
)
def new_event_info() -> pd.DataFrame:
    with snowflake.connector.connect(
            connection_name="stg"
        ) as con:
            _sql = """select event_id 
                    from pdga_db.pdga_stg.EVENT_REQUESTS s
                    --where event_id = 87901
                    union
                    select event_id
                    from pdga_db.pdga.EVENT_REQUESTS e
                    where e.status = 2
                    minus
                    select event_id
                    from pdga_db.pdga.EVENT_REQUESTS e
                    where e.status in (3,4)"""
            pending_events = con.cursor().execute(_sql) # this feels so wrong, can i make this a dbt query?
            results = pd.DataFrame()
            for event in pending_events:
                event_info = event_info_extractor(event[0])
                if event_info is not None:
                    results = pd.concat([results, event_info])
            if len(results) > 0:
                print(f"Writing {len(results)} new events to EVENTS_INFO")
                write_pandas(con, results, "EVENTS_INFO", auto_create_table=True, overwrite=True)
    return results