#!/usr/bin/env python
# coding: utf-8


from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from io import StringIO

import pandas as pd
import requests
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# Variables
DATAGOUV_URL = Variable.get("DATAGOUV_URL")
DATAGOUV_API_KEY = Variable.get('DATAGOUV_API_KEY')
CREATE_POST_BY_API = Variable.get('CREATE_POST_BY_API', False)


# Previous month formats
DAG_NAME = 'automated_edito_post_dag'
NOW = date.today()
LAST_MONTH_DATE = NOW + relativedelta(months=-1)
LAST_MONTH_DATE_FMT = LAST_MONTH_DATE.strftime("%Y-%m")
MONTHS = ['Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin', 'Juillet',
          'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre']
LAST_MONTH_DATE_STR = f'{MONTHS[LAST_MONTH_DATE.month - 1]} {LAST_MONTH_DATE.strftime("%Y")}'


def create_edito_post():

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
            out += f'        <div data-udata-dataset="{slug}"></div>\n'
        out += '    </div>\n'
        return out

    def list_reuses(reuses):
        out = '<div class="fr-my-2w fr-grid-row fr-grid-row--gutters">\n'
        for slug in reuses:
            out += f'        <div data-udata-reuse="{slug}"></div>\n'
        out += '    </div>\n'
        return out

    content = f"""
    <script data-udata="https://www.data.gouv.fr/" src="https://static.data.gouv.fr/static/oembed.js" async defer></script>

    <p>En {LAST_MONTH_DATE_STR}, {new_datasets_count} jeux de données et {new_reuses_count} réutilisations ont été publiés sur data.gouv.fr.</p>
    <a href="http://activites-datagouv.app.etalab.studio/"  target="_blank">Découvrez plus de statistiques sur la plateforme</a>.

    <div class="fr-my-6w">
        <h3 >Les jeux de données du mois</h3>
        <p>Les jeux de données qui ont retenus notre attention ce mois-ci :</p>
        {list_datasets(featured_datasets_slug)}
    </div>

    <div class="fr-my-6w">
        <h3>Les réutilisations du mois</h3>
        <p>Les réutilisations qui ont retenus notre attention ce mois-ci :</p>
        {list_reuses(featured_reuses_slug)}
    </div>

    <div class="fr-my-6w">
        <h3>Les tendances du mois sur data.gouv.fr</h3>
        <p>Les jeux de données publiés ce mois-ci les plus populaires :</p>
        {list_datasets(trending_datasets_slug)}
        <p>Les réutilisations publiées ce mois-ci les plus populaires :</p>
        {list_reuses(trending_reuses_slug)}
    </div>


    <h3>Suivez l’actualité de la plateforme</h3>
    <p>Le suivi des sorties n’est que le sommet de l’iceberg de l’activité de data.gouv.fr.
    Pour ne rien manquer, de l’actualité de data.gouv.fr et de l’open data, 
    <a href="https://infolettres.etalab.gouv.fr/subscribe/rn7y93le1"  target="_blank">inscrivez-vous à notre infolettre</a>. 
    pour experimenter les nouveauté de la plateforme en avant première et nous aider à l’améliorer n’hésitez pas à
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

        r = requests.post(f"{Variable.get('DATAGOUV_URL')}/api/1/posts/", headers=headers, json={
            'name': name,
            'headline': headline,
            'content': content,
            'body_type': 'html',
            'tags': ['suivi-des-sorties']
        })
        post_id = r.json()['id']
        print(f'Article créé et éditable à {DATAGOUV_URL}/admin/post/{post_id}')


with DAG(
    dag_id=DAG_NAME,
    schedule_interval='0 8 1 * *',
    start_date=days_ago(31),
    dagrun_timeout=timedelta(minutes=60),
    tags=['test'],
) as dag:
   
    edito = PythonOperator(
        task_id="create_edito_post",
        python_callable=create_edito_post,
    )

    edito
