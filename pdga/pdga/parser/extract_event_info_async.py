from bs4 import BeautifulSoup
import pandas as pd
import aiohttp
import asyncio
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

async def event_info_extractor(session: aiohttp.ClientSession, event_id):
    url = f"https://www.pdga.com/tour/event/{event_id}"
    try:
        async with session.get(url) as resp:
            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")

            records = []

            # first lets get tourny level info
            event_name = soup.find('h1', attrs={'id':"page-title"}).text.replace('/','_')
            event_info = soup.find_all("div", attrs={"class":"pane-tournament-event-info"})[0]
            event_date = event_info.find('li', attrs={'class':"tournament-date"}).text.split(':')[1].strip()
            event_location = event_info.find('li', attrs={'class':"tournament-location"}).text.split(":")[1].strip()
            event_city, event_state, event_country = event_location.split(",")
            event_director = event_info.find('li', attrs={'class': "tournament-director"}).text.split(":")[1].strip()
            event_type = event_info.find("h4").text
            event_view = soup.find_all("div", attrs={"class":"pane-tournament-event-view"})
            if event_view is not None:
                event_status = event_view[0].find("td", attrs={"class":"status"}).text
                if "Event complete" not in event_status:
                    # event not finalized, dont gather data
                    print('Event still pending', event_id)
                    return None
                event_player_count = event_view[0].find("td", attrs={"class":"players"}).text
                event_purse = event_view[0].find("td", attrs={"class":"purse"}).text
            else:
                event_player_count = ""
                event_purse = ""
            
            #find each of the divisions in the event
            divisions = soup.find_all('details')
            for division in divisions:
                # each division can play at a different location/layout
                div_id = division.find("h3")["id"]
                rounds = division.find_all("th", attrs={"class":"round"})
                for round in rounds:
                    # each round can be played at a different location/layout
                    round_id = round.text.strip("Rd")
                    round_info = division.find("div", attrs={"id":f"layout-details-{event_id}-{div_id}-round-{round_id}"}).text
                    round_course = round_info.split(";")[0].split("-")[0].strip()
                    round_layout = round_info.split(";")[0].split("-")[1].strip()
                    round_holes = round_info.split(";")[1].split()[0].strip()
                    round_par = round_info.split(";")[2].split()[1].strip()
                    round_dist = round_info.split(";")[3].strip()
                    # get each players score for this round
                    players = division.find_all("tr")
                    for player in players:
                        if "th" in player.contents[0].name:
                            continue
                        player_points = player.find("td", attrs={"class":"points"}).text
                        player_pdga = player.find("td", attrs={"class":"pdga-number"}).text
                        player_rating = player.find("td", attrs={"class":"player-rating"}).text
                        player_round_score = player.find_all("td", attrs={"class":"round"})[int(round_id)-1].text
                        player_round_rating = player.find_all("td", attrs={"class":"round-rating"})[int(round_id)-1].text
                        # add all the info to the record list
                        records.append([
                            event_name,
                            event_date,
                            event_city,
                            event_state,
                            event_country,
                            event_director,
                            event_type,
                            event_player_count,
                            event_purse,
                            div_id,
                            round_id,
                            round_course,
                            round_layout,
                            round_holes,
                            round_par,
                            round_dist,
                            player_pdga,
                            player_points,
                            player_rating,
                            player_round_score,
                            player_round_rating
                        ])

            df = pd.DataFrame(columns=[
                "event_name",
                "event_date",
                "event_city",
                "event_state",
                "event_country",
                "event_director",
                "event_type",
                "event_player_count",
                "event_purse",
                "event_division",
                "round_number",
                "round_course",
                "round_layout",
                "layout_holes",
                "layout_par",
                "layout_distance",
                "player_pdga",
                "player_earned_points",
                "player_rating",
                "player_round_score",
                "player_round_rating"
            ], data=records)
    except AttributeError as e:
        print("Error loading information for", event_id, e)
        return None

    return df

async def main():    
    
    # process event
    # get pending events from db (eventually filter out dups and pick up pendings)
    # create task list
    # wait for all the complete
    # save df to stg
    async with aiohttp.ClientSession() as session:
        tasks = []
        # i dislike how this is tightly coupled with sf, maybe make interface/subclasses
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
            for event in pending_events:
                tasks.append(asyncio.create_task(event_info_extractor(session, event[0]), name=event[0]))
            completed_events, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
            print(completed_events)
            events_info = [event.result() for event in completed_events if event.result() is not None]
            for task in pending:
                print(f"{task.get_name()} still pending")
                task.cancel()
            print(events_info)
            if len(events_info) > 0:
                print(f"Writing {len(events_info)} new events to EVENTS_INFO")
                results = pd.concat(events_info)
                write_pandas(con, results, "EVENTS_INFO", auto_create_table=True, overwrite=True)

if __name__ == '__main__':
    asyncio.run(main())