from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.operators.dummy_operator import DummyOperator

import dwh_import
from configuration import config
from datahub.common import alerts
from datahub.operators.bigquery.bigquery_create_empty_dataset_operator import BigQueryCreateEmptyDatasetOperator
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator

template_search_path = '{}/rps_korean_team_dataset/sql'.format(configuration.get('core', 'dags_folder'))
version = 1
default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2020, 2, 17, 0, 0, 0),
    'retries': 2,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

doc_md = f"""#### Create a snapshot for defined rps tables for the Korean team."""

dataset = config.get('rps_korean_team_dataset').get('dataset')
project_id = config.get('rps_korean_team_dataset').get('project_id')

with DAG(
        dag_id=f'rps-korean-team-dataset-v{version}',
        description=f'Creates a snapshot for defined rps tables for the Korean team',
        schedule_interval='30 23 * * *',
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        template_searchpath=template_search_path,
        tags=[dwh_import.DEFAULT_ARGS['owner'], 'dwh'],
        catchup=False) as dag:
    dag.doc_md = doc_md

    start = DummyOperator(
        task_id='start'
    )

    create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id=f'create-dataset-{dataset}',
        dataset_id=dataset,
        project_id=project_id,
        on_failure_callback=alerts.setup_callback(),
        execution_timeout=timedelta(minutes=5),
    )

    for table in config.get('rps_korean_team_dataset').get('tables'):
        table_name = table.get('name')
        time_partitioning_column = table.get('time_partitioning', None)
        time_partitioning = None
        if time_partitioning_column is not None:
            time_partitioning = {
                'field': time_partitioning_column,
                'type': 'DAY',
            }
        create_table = BigQueryOperator(
            task_id=f'create-{table_name}',
            sql=f'{table_name}.sql',
            destination_dataset_table=f'{dataset}.{table_name}',
            time_partitioning=time_partitioning,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
        )
        start >> create_dataset_task >> create_table
