from bs4 import BeautifulSoup
import pandas as pd
from requests import get
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def event_info_extractor(event_id):
    url = f"https://www.pdga.com/tour/event/{event_id}"
    try:
        with get(url) as page:
            soup = BeautifulSoup(page.content, "html.parser")

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
                try:
                    event_purse = event_view.find("td", attrs={"class":"purse"}).text
                except AttributeError:
                    event_purse = ""
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
                        try:
                            player_points = player.find("td", attrs={"class":"points"}).text
                        except AttributeError:
                            player_points = ""
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

def main():    
    
    # process event
    # get pending events from db (eventually filter out dups and pick up pendings)
    # create task list
    # wait for all the complete
    # save df to stg
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

if __name__ == '__main__':
    main()