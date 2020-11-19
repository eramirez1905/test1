from datetime import datetime, timedelta

from airflow import DAG, configuration

import dwh_import
from feature_store.feature_store import FeatureStore

version = 1
template_search_path = '{}/feature_store/sql'.format(configuration.get('core', 'dags_folder'))
default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2019, 12, 1, 0, 0, 0),
    'retries': 2,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}
doc_md = "#### Queued orders backfill DAG"

with DAG(
        dag_id=f'feature-store-backfill-v{version}',
        description='Backfills the queued orders historic table. ',
        schedule_interval=None,
        template_searchpath=template_search_path,
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        tags=[dwh_import.DEFAULT_ARGS['owner'], 'dwh'],
        catchup=False) as dag:
    dag.doc_md = doc_md

    feature_store = FeatureStore(full_import=True)
    feature_store.render()
