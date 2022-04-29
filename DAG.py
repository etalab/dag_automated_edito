import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import tweepy

from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta

DAG_NAME = 'tweet_dag'
NOW = datetime.now()
BEGINNING_DATE = NOW + relativedelta(months=-1)
BEGINNING_DATE_STR = BEGINNING_DATE.strftime("%Y-%m-%d")


CONSUMER_KEY = Variable.get('twitter_consumer_key')
CONSUMER_KEY_SECRET = Variable.get('twitter_consumer_key_secret')
ACCESS_TOKEN = Variable.get('twitter_access_token')
ACCESS_SECRET_TOKEN = Variable.get('twitter_secret_token_secret')


def tweet_featured_from_catalog(url, phrase_intro):

    authenticator = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_KEY_SECRET)
    authenticator.set_access_token(ACCESS_TOKEN, ACCESS_SECRET_TOKEN)

    api = tweepy.API(authenticator, wait_on_rate_limit=True)

    df = pd.read_csv(url, sep=";")
    nb_items = df[(df['created_at'] > BEGINNING_DATE_STR)].shape[0]
    df = df[(df['created_at'] > BEGINNING_DATE_STR) & (df['featured'] == True)]
    df['title_bis'] = df['title'].apply(lambda x: x[:215] + '[...]' if len(x) > 215 else x)
    df['tweet'] = df['title_bis'] + ' https://data.gouv.fr/fr/datasets/' + df['id']

    tweets = list(df['tweet'].unique())

    intro = 'Ce mois-ci, ' + str(nb_items) + ' ' + phrase_intro + ' sur data.gouv.fr. \n Découvrez nos coups de coeur dans ce fil #opendata \n 🔽🔽🔽🔽'

    #tweets = intro + tweets
    original_tweet = api.update_status(status=intro)
    reply_tweet = original_tweet

    for tweet in tweets:
        reply_tweet = api.update_status(status=tweet, 
                                 in_reply_to_status_id=reply_tweet.id, 
                                 auto_populate_reply_metadata=True)


def process_tweeting():

    tweet_featured_from_catalog('https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3', 'jeux de données ont été publiés')
    tweet_featured_from_catalog('https://www.data.gouv.fr/fr/datasets/r/970aafa0-3778-4d8b-b9d1-de937525e379', 'réutilisations ont été publiées')


with DAG(
    dag_id=DAG_NAME,
    schedule_interval='0 8 1 * *',
    start_date=days_ago(31),
    dagrun_timeout=timedelta(minutes=60),
    tags=['test'],
) as dag:
   
    tweet = PythonOperator(
        task_id="test_task",
        python_callable=process_tweeting,
    )

    tweet



