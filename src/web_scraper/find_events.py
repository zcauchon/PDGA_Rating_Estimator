from bs4 import BeautifulSoup
from requests import get
import boto3
from datetime import date, timedelta

#since pdga wont let me use their api ill use their public search
#https://www.pdga.com/tour/search?date_filter[min][date]=2022-01-29&date_filter[max][date]=2023-01-29&State[]=PA&Tier[]=A&Tier[]=B&Tier[]=C

def lambda_handler(event, context):
#if __name__ == '__main__':
    d = date.today() - timedelta(days=15)
    min_date = d.replace(day=1).strftime('%Y-%m-%d')
    url = f'https://www.pdga.com/tour/search?date_filter[min][date]={min_date}&State[]=PA&Tier[]=A&Tier[]=B&Tier[]=C'
    event_url_base = 'https://www.pdga.com'
    l = boto3.client('lambda')
    page = get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    for event in soup.find_all('td', attrs={'class':'views-field-OfficialName'}):
        event_url = f'{event_url_base}{event.a["href"]}'
        print('Processing', event_url)
        l.invoke(
            FunctionName='arn:aws:lambda:us-east-2:628608553663:function:scrape_pdga_round',
            InvocationType='Event',
            Payload='{"url":"' + event_url + '"}'
        )
