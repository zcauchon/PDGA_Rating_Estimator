from bs4 import BeautifulSoup
from requests import get
from datetime import date, timedelta
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

#since pdga wont let me use their api ill use their public search
#https://www.pdga.com/tour/search?date_filter[min][date]=2022-01-29&date_filter[max][date]=2023-01-29&State[]=PA&Tier[]=A&Tier[]=B&Tier[]=C

if __name__ == '__main__':
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
    with snowflake.connector.connect(
        connection_name="stg"
    ) as con:
        success, nchunks, nrows, _ = write_pandas(con, df, 'EVENT_REQUESTS', auto_create_table=True, overwrite=True)
        print(f'Loaded {nrows} rows for processing')