from datetime import datetime, timedelta

from airflow import DAG, configuration

import dwh_import
from braze_import.braze import Braze

template_search_path = '{}/braze_import/sql'.format(configuration.get('core', 'dags_folder'))
version = 1
default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2019, 12, 10, 0, 0, 0),
    'retries': 2,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

doc_md = "#### Get the latest riders and push payload to Braze API."

with DAG(
        dag_id=f'braze-import-daily-v{version}',
        description='Query BigQuery and push payload to Braze API.',
        schedule_interval='0 12 * * *',
        template_searchpath=template_search_path,
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        tags=[dwh_import.DEFAULT_ARGS['owner'], 'dwh'],
        catchup=False) as dag:
    dag.doc_md = doc_md

    braze = Braze(full_import=False)
    braze.render()
