from datetime import datetime, timedelta

from airflow import DAG, conf

from configuration import config
from configuration.default_params import DAG_DEFAULT_ARGS
from datahub.curated_data.entities_config import EntitiesConfig
from datahub.curated_data.process_curated_data import ProcessCuratedData
from datahub.dwh_import_read_replica import DwhImportReadReplica

version = 1

default_args = {
    'owner': 'data-bi',
    # TODO Adjust based on your needs
    'start_date': datetime(2019, 6, 4),
    'retries': 1,
    'concurrency': 20,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
    'executor_config': {
        'KubernetesExecutor': {
            'request_cpu': "200m",
            'limit_cpu': "200m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi",
        },
    },
}

dwh_import_read_replicas = DwhImportReadReplica(config)
dags_folder = conf.get('core', 'dags_folder')

with DAG(
        dag_id=f'curated-data-backfill-v{version}',
        description=f'Backfill Curated Data on BigQuery',
        schedule_interval=None,
        default_args={**DAG_DEFAULT_ARGS, **default_args},
        max_active_runs=1,
        template_searchpath=f'{dags_folder}/curated_data/sql/',
        tags=[default_args['owner'], 'dwh'],
        catchup=False) as dag:

    etl = ProcessCuratedData(
        dag=dag,
        project_id=config.get("bigquery").get("project_id"),
        dataset_id=config.get('bigquery').get('dataset').get('cl'),
        config=config.get('curated_data'),
        entities=EntitiesConfig().entities,
        policy_tags=config.get('policy_tags', []),
        create_daily_tasks=False,
        backfill=True,
        dwh_import=dwh_import_read_replicas,
    )
    etl.render()
