import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests

NOW = datetime.now()
BEGINNING_DATE = NOW + relativedelta(months=-1)

BEGINNING_DATE_STR = BEGINNING_DATE.strftime("%Y-%m-%d")


def tweet_featured_from_catalog(url):
    df = pd.read_csv(url, sep=";")
    nb_items = df[(df['created_at'] > BEGINNING_DATE_STR)].shape[0]
    df = df[(df['created_at'] > BEGINNING_DATE_STR) & (df['featured'] == True)]
    df['title_bis'] = df['title'].apply(lambda x: x[:215] + '[...]' if len(x) > 215 else x)
    df['tweet'] = df['title_bis'] + ' https://data.gouv.fr/fr/datasets/' + df['id']

    tweets = list(df['tweet'].unique())

    intro = ['Ce mois-ci, ' + str(nb_items) + ' jeux de données ont été publiées sur data.gouv.fr. \n Découvrez nos coups de coeur dans ce fil #opendata \n :arrow_heading_down: :arrow_heading_down: :arrow_heading_down:']

    tweets = intro + tweets

    for tweet in tweets:
        data = {
            'text': tweet
        }
        r = requests.post('https://mattermost.incubateur.net/hooks/geww4je6minn9p9m6qq6xiwu3a', json = data)        
        print(tweet)


tweet_featured_from_catalog('https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3')
tweet_featured_from_catalog('https://www.data.gouv.fr/fr/datasets/r/970aafa0-3778-4d8b-b9d1-de937525e379')
