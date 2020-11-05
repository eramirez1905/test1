from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.operators.latest_only_operator import LatestOnlyOperator

import dwh_import
from business_intelligence_etl.process_etl import populate_etl
from configuration import config
from datahub.common.bigquery_task_generator import BigQueryTaskGenerator
from datahub.curated_data.entities_config import EntitiesConfig
from datahub.curated_data.process_curated_data import ProcessCuratedData
from datahub.dwh_import_read_replica import DwhImportReadReplica

version = 5

default_args = {
    'owner': 'data-bi',
    'start_date': datetime(2020, 8, 4, 5, 0, 0),
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
dag_id_prefix = 'curated-data'

doc_md = f"""
#### Create Curated Data on BigQuery
"""

dwh_import_read_replicas = DwhImportReadReplica(config)

with DAG(
        dag_id=f'{dag_id_prefix}-v{version}',
        description=f'Create Curated Data on BigQuery',
        schedule_interval=config.get('dags').get(dag_id_prefix, {}).get('schedule_interval', '30 0,5,8,20 * * *'),
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
        config=config.get('curated_data'),
        entities=EntitiesConfig().entities,
        policy_tags=config.get('policy_tags', []),
        create_daily_tasks=False,
        dwh_import=dwh_import_read_replicas,
    )
    tasks = etl.render()

    # Legacy IL tables
    generator = BigQueryTaskGenerator(
        dag=dag,
        config=config,
        prefix='il-',
        search_path='business_intelligence_etl/sql',
        curated_data_tasks=tasks,
    )
    populate_etl(generator)

    il_tasks = generator.render()
    start = LatestOnlyOperator(
        dag=dag,
        task_id='start-il'
    )
    for operator in il_tasks.values():
        if len(operator.upstream_list) == 0:
            start.set_downstream(operator)
