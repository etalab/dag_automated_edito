import pandas as pd
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from io import StringIO
import requests
import tweepy

from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta

DAG_NAME = 'automated_edito_post_and_tweet'
NOW = datetime.now()

NOW = date.today()
LAST_MONTH_DATE = NOW + relativedelta(months=-1)
LAST_MONTH_DATE_FMT = LAST_MONTH_DATE.strftime("%Y-%m")
MONTHS = ['Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin', 'Juillet',
          'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre']
LAST_MONTH_DATE_STR_SHORT = f'{MONTHS[LAST_MONTH_DATE.month - 1]}'
LAST_MONTH_DATE_STR = f'{MONTHS[LAST_MONTH_DATE.month - 1]} {LAST_MONTH_DATE.strftime("%Y")}'


DATAGOUV_URL = Variable.get("DEV_DATAGOUV_URL")
DATAGOUV_API_KEY = Variable.get('DEV_DATAGOUV_SECRET_API_KEY')
CREATE_POST_BY_API = Variable.get('CREATE_POST_BY_API', False)
CONSUMER_KEY = Variable.get('TWITTER_CONSUMER_KEY')
CONSUMER_KEY_SECRET = Variable.get('TWITTER_CONSUMER_KEY_SECRET')
ACCESS_TOKEN = Variable.get('TWITTER_ACCESS_TOKEN')
ACCESS_SECRET_TOKEN = Variable.get('TWITTER_SECRET_TOKEN')
MATTERMOST_EDITO_URL = Variable.get('MATTERMOST_DATAGOUV_EDITO')


def tweet_featured_from_catalog(url, obj_type, phrase_intro):

    authenticator = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_KEY_SECRET)
    authenticator.set_access_token(ACCESS_TOKEN, ACCESS_SECRET_TOKEN)

    api = tweepy.API(authenticator, wait_on_rate_limit=True)

    df = pd.read_csv(url, sep=";")
    nb_items = df[(df['created_at'].str.match(LAST_MONTH_DATE_FMT))].shape[0]
    df = df[(df['created_at'].str.match(LAST_MONTH_DATE_FMT)) & (df['featured'] == True)]
    df['title_bis'] = df['title'].apply(lambda x: x[:215] + '[...]' if len(x) > 215 else x)
    df['tweet'] = df['title_bis'] + ' https://data.gouv.fr/fr/' + obj_type + '/' + df['id']

    tweets = list(df['tweet'].unique())

    intro = 'En ' + LAST_MONTH_DATE_STR_SHORT + ', ' + str(nb_items) + ' ' + phrase_intro + ' sur data.gouv.fr. \n Découvrez nos coups de coeur dans ce fil #opendata \n 🔽🔽🔽🔽'

    #tweets = intro + tweets
    original_tweet = api.update_status(status=intro)
    
    reply_tweet = original_tweet

    for tweet in tweets:
        reply_tweet = api.update_status(status=tweet, 
                                 in_reply_to_status_id=reply_tweet.id, 
                                 auto_populate_reply_metadata=True)

    return ':bird: Thread sur les ' + obj_type + ' du mois dernier publié [ici](https://twitter.com/DatagouvBot/status/' + str(original_tweet.id) + ')'


def process_tweeting(**kwargs):

    dataset_thread = tweet_featured_from_catalog('https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3', 'datasets', 'jeux de données ont été publiés')
    reuse_thread = tweet_featured_from_catalog('https://www.data.gouv.fr/fr/datasets/r/970aafa0-3778-4d8b-b9d1-de937525e379', 'reuses', 'réutilisations ont été publiées')

    kwargs["ti"].xcom_push(key='published_threads', value=[dataset_thread, reuse_thread]) 


    
def create_edito_post(**kwargs):

    # Get datasets and reuses from catalog
    def load_catalog(url):
        r = requests.get(url)
        return pd.read_csv(StringIO(r.text), delimiter=';')

    df_dataset = load_catalog('https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3')
    df_reuse = load_catalog('https://www.data.gouv.fr/fr/datasets/r/970aafa0-3778-4d8b-b9d1-de937525e379')

    # Get featured
    featured_datasets_slug = df_dataset[df_dataset.created_at.str.match(LAST_MONTH_DATE_FMT)&df_dataset.featured].slug.values
    featured_reuses_slug = df_reuse[df_reuse.created_at.str.match(LAST_MONTH_DATE_FMT)&df_reuse.featured].slug.values

    # Get new counts
    new_datasets_count = df_dataset[df_dataset.created_at.str.match(LAST_MONTH_DATE_FMT)].shape[0]
    new_reuses_count = df_reuse[df_reuse.created_at.str.match(LAST_MONTH_DATE_FMT)].shape[0]

    # Get trending datasets and reuses
    def load_trending(url):
        return requests.get(url).json()

    trending_datasets_url = 'https://object.files.data.gouv.fr/opendata/datagouv/dashboard/tops-trending-datasets-weekly.json'
    trending_reuses_url = 'https://object.files.data.gouv.fr/opendata/datagouv/dashboard/tops-trending-reuses-weekly.json'

    trending_datasets_slug = [res['url'].split('/')[-2] for res in load_trending(trending_datasets_url)['values'][:6]]
    trending_reuses_slug = [res['url'].split('/')[-2] for res in load_trending(trending_reuses_url)['values'][:6]]

    # Generate HTML
    def list_datasets(datasets):
        out = '<div class="fr-my-2w">\n'
        for slug in datasets:
            out += f'        <div class="udata-oembed--border-bottom" data-udata-dataset="{slug}"></div>\n'
        out += '    </div>\n'
        return out

    def list_reuses(reuses):
        out = '<div class="fr-my-2w fr-grid-row fr-grid-row--gutters">\n'
        for slug in reuses:
            out += f'        <div class="udata-oembed--border-bottom fr-col-lg-3 fr-col-sm-6 fr-col-12" data-udata-reuse="{slug}"></div>\n'
        out += '    </div>\n'
        return out

    content = f"""
    <script data-udata="https://www.data.gouv.fr/" src="https://static.data.gouv.fr/static/oembed.js" async defer></script>

    <h3>En {LAST_MONTH_DATE_STR}, {new_datasets_count} jeux de données et {new_reuses_count} réutilisations ont été publiés sur data.gouv.fr.</h3>
    <a href="http://activites-datagouv.app.etalab.studio/"  target="_blank">Découvrez plus de statistiques sur l'activité de la plateforme</a>.
    <p>Retrouvez-ici nos jeux de données et réutilisations coups de coeur du mois, ainsi que les publications récentes les plus populaires en {LAST_MONTH_DATE_STR}.</p>
    <div class="fr-my-6w">
        <h3 >Les jeux de données du mois</h3>
        <p>Les jeux de données qui ont retenu notre attention ce mois-ci :</p>
        {list_datasets(featured_datasets_slug)}
    </div>

    <div class="fr-my-6w">
        <h3>Les réutilisations du mois</h3>
        <p>Les réutilisations qui ont retenu notre attention ce mois-ci :</p>
        {list_reuses(featured_reuses_slug)}
    </div>

    <div class="fr-my-6w">
        <h3>Les tendances du mois sur data.gouv.fr</h3>
        <p>Les jeux de données publiés ce mois-ci les plus populaires :</p>
        {list_datasets(trending_datasets_slug)}
        <p>Il s'agit des jeux de données et des réutilisations créés récemment les plus consultés au mois de {LAST_MONTH_DATE_STR}.</p>
        <p>Les réutilisations publiées ce mois-ci les plus populaires :</p>
        {list_reuses(trending_reuses_slug)}
    </div>


    <h3>Suivez l’actualité de la plateforme</h3>
    <p>Le suivi des sorties ne constitue que le sommet de l’iceberg de l’activité de data.gouv.fr.
    Pour ne rien manquer de l’actualité de data.gouv.fr et de l’open data, 
    <a href="https://infolettres.etalab.gouv.fr/subscribe/rn7y93le1"  target="_blank">abonnez-vous à notre infolettre</a>. 
    <br />Et si vous souhaitez nous aider à améliorer la plateforme en testant les nouveautés en avant première, n’hésitez pas à
    <a href="https://app.evalandgo.com/s/index.php?id=JTk5biU5OWolOUQlQUI%3D&a=JTk3cCU5M2glOTklQUU%3D"  target="_blank">devenir beta testeur</a>.</p>
    """

    print(content)

    # Create a POST
    if CREATE_POST_BY_API:
        headline = f"Vous lisez l’édition {'d’' if LAST_MONTH_DATE_STR.startswith('a') or LAST_MONTH_DATE_STR.startswith('o') else 'de '}{LAST_MONTH_DATE_STR} du suivi des sorties, un article dans lequel nous partageons les publications des jeux de données et des réutilisations qui ont retenus notre attention."
        name = f'Suivi des sorties - {LAST_MONTH_DATE_STR}'

        headers = {
            'X-Api-Key': DATAGOUV_API_KEY
        }

        r = requests.post(f"{DATAGOUV_URL}/api/1/posts/", headers=headers, json={
            'name': name,
            'headline': headline,
            'content': content,
            'body_type': 'html',
            'tags': ['suivi-des-sorties']
        })
        post_id = r.json()['id']
        print(f'Article créé et éditable à {DATAGOUV_URL}/admin/post/{post_id}')

        kwargs["ti"].xcom_push(key='admin_post_url', value=f':rolled_up_newspaper: Article du {name} créé et éditable [dans l\'espace admin]({DATAGOUV_URL}/admin/post/{post_id})') 



def publish_mattermost(ti):

    published_threads=ti.xcom_pull(key='published_threads', task_ids='tweet_threads')
    admin_post_url=ti.xcom_pull(key='admin_post_url', task_ids='create_edito_post')
    
    print(published_threads)
    print(admin_post_url)

    data = {
        'text': ':mega: @agarrone @thanh-ha.le \n - ' + admin_post_url + ' \n - ' + '\n - '.join(published_threads) 
    }

    r = requests.post(MATTERMOST_EDITO_URL, json = data)
    print(data)

default_args = {
   'email': ['geoffrey.aldebert@data.gouv.fr'],
   'email_on_failure': True
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='0 8 1 * *',
    start_date=days_ago(31),
    dagrun_timeout=timedelta(minutes=60),
    tags=['edito', 'mattermost', 'post', 'twitter'],
    default_args=default_args,
) as dag:
   
    edito = PythonOperator(
        task_id="create_edito_post",
        python_callable=create_edito_post,
    )

    tweet = PythonOperator(
        task_id="tweet_threads",
        python_callable=process_tweeting,
    )

    mattermost = PythonOperator(
        task_id = 'publish_mattermost',
        python_callable=publish_mattermost
    )

    [tweet, edito] >> mattermost
