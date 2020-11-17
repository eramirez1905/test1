from datetime import datetime, timedelta

from airflow import DAG, configuration

from configuration import config
from datahub.common.helpers import DatabaseSettings
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'concurrency': 1,
    'max_active_runs': 1,
    'executor_config': {
        'KubernetesExecutor': {
            'request_cpu': "200m",
            'limit_cpu': "200m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi",
        },
    },
    'start_date': datetime(2019, 4, 1),
    'end_date': None,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

template_search_path = '{}/delete-sg-dg/sql/'.format(configuration.get('core', 'dags_folder'))

with DAG(dag_id='delete-dp-sg',
         description='Delete dp_sg country_code data',
         schedule_interval=None,
         default_args=default_args,
         max_active_runs=1,
         template_searchpath=template_search_path,
         catchup=False) as dag:
    for database_name, params in config['dwh_merge_layer_databases'].items():
        database = DatabaseSettings(database_name, params)
        if database.name in ['hurrier', 'rooster', 'porygon']:
            for table in database.tables:
                bigquery_operator = BigQueryOperator(
                    dag=dag,
                    task_id=f'delete-{table.table_name}',
                    params={
                        'table_name': table.table_name,
                        'project_id': config.get("bigquery").get("project_id")
                    },
                    sql='delete-dp_sg.sql',
                    use_legacy_sql=False,
                    execution_timeout=timedelta(minutes=15),
                )
