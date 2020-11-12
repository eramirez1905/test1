from datetime import datetime, timedelta

from airflow import DAG, configuration

import dwh_import
from configuration import config
from datahub.curated_data.entities_config import EntitiesConfig
from datahub.curated_data.process_curated_data import ProcessCuratedData
from datahub.dwh_import_read_replica import DwhImportReadReplica

version = 1

default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2019, 6, 4),
    'retries': 2,
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

dags_folder = configuration.get('core', 'dags_folder')

doc_md = f"""
#### Create Curated Data on BigQuery
"""


def merge_curated_data_config(curated_data, curated_data_hourly):
    curated_data = curated_data.copy()
    curated_data_hourly_log = curated_data_hourly.get('logistics', {'tables': []}).get('tables')
    curated_data_hourly_rps = curated_data_hourly.get('rps', {'tables': []}).get('tables')
    curated_data.get('rps').get('tables', []).extend(curated_data_hourly_rps)
    curated_data.get('logistics').get('tables', []).extend(curated_data_hourly_log)

    return curated_data


dwh_import_read_replicas = DwhImportReadReplica(config)

curated_data_config = merge_curated_data_config(
    config.get('curated_data'),
    config.get('curated_data_hourly')
)
# Hack to include curated_data_dps_user_sessions config
curated_data_config = merge_curated_data_config(
    curated_data_config,
    config.get('curated_data_dps_user_sessions')
)

with DAG(
        dag_id=f'curated-data-daily-v{version}',
        description=f'Create Curated Data on BigQuery',
        schedule_interval='30 10 * * *',
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        max_active_runs=1,
        template_searchpath=f'{dags_folder}/curated_data/sql/',
        tags=[default_args['owner'], 'dwh'],
        catchup=False) as dag:
    dag.doc_md = doc_md

    etl = ProcessCuratedData(
        dag=dag,
        project_id=config.get("bigquery").get("project_id"),
        dataset_id=config.get('bigquery').get('dataset').get('cl'),
        config=curated_data_config,
        entities=EntitiesConfig().entities,
        policy_tags=config.get('policy_tags', []),
        create_daily_tasks=True,
        dwh_import=dwh_import_read_replicas,
    )
    tasks = etl.render()
