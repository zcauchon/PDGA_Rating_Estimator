from bs4 import BeautifulSoup
from requests import get
import pandas as pd
import awswrangler as wr


def lambda_handler(event, context):
    #if __name__ == '__main__':
    course_names = []
    course_layouts = []
    course_pars = []
    course_distances = []
    round_scores = []
    round_ratings = []
    page = get(event['url'])
    soup = BeautifulSoup(page.content, "html.parser")
    #find each of the division results
    tourny_name = soup.find('h1', attrs={'id':"page-title"}).text.replace('/','_')
    tourny_date = soup.find('li', attrs={'class':"tournament-date"}).text.split(':')[1].strip()
    division = soup.find_all('details')
    for div in division:
        layout_spans = div.find_all('span')
        for span in layout_spans:
            #find span that has the layout details
            if span.get('id'):
                if 'layout-details' in span.get('id'):
                    course_name: str = span.a.string
                    course_info: str = span.text.split(' - ')[1].split(';')
                    course_layout = course_info[0].strip()
                    course_par = course_info[2].strip()
                    course_distance = course_info[3].strip()
        round_score = None
        round_rating = None
        players = span.find_next('tbody')
        for td in players.find_all('td'):
            #find round score
            if 'round' in td.get('class'):
                round_score = td.a.string
            # find round raiting
            elif 'round-rating' in td.get('class'):
                round_rating = td.string
            if all(v is not None for v in [round_score, round_rating]):
                round_scores.append(round_score)
                round_ratings.append(round_rating)
                course_names.append(course_name)
                course_layouts.append(course_layout)
                course_pars.append(course_par)
                course_distances.append(course_distance)
                round_score = None
                round_rating = None
    df = pd.DataFrame(data={
        'tourny':[tourny_name]*len(course_names),
        'date':[tourny_date]*len(course_names),
        'course':course_names, 
        'layout':course_layouts,
        'par':course_pars, 
        'distance':course_distances, 
        'score':round_scores, 
        'rating':round_ratings}
    )
    df.drop_duplicates(inplace=True)
    for course in df['course'].unique():
        course_df = df[df['course'] == course]
        wr.s3.to_csv(course_df, f's3://pdga-ratings/{course}/{tourny_name}_{tourny_date}.csv', index=False)