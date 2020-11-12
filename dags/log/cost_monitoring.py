from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.operators.dummy_operator import DummyOperator

import dwh_import
from configuration import config
from datahub.common import alerts
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from datahub.operators.cost_monitoring_operator import CostMonitoringOperator

template_search_path = '{}/cost_monitoring/sql'.format(configuration.get('core', 'dags_folder'))
version = 1
default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2020, 1, 27, 0, 0, 0),
    'retries': 3,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

doc_md = "#### Cost monitoring DAG."

with DAG(
        dag_id=f'cost-monitoring-v{version}',
        description='DAG which monitors and alters expensive queries.',
        schedule_interval='30 8 * * *',
        template_searchpath=template_search_path,
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        tags=[dwh_import.DEFAULT_ARGS['owner'], 'dwh'],
        catchup=False) as dag:
    dag.doc_md = doc_md

    start = DummyOperator(task_id='start')

    params = {
        'project_id': config.get('cost_monitoring').get('project_id'),
        'dataset': 'billing',
        'view_name': 'logs',
        'source_table_name': 'cloudaudit_googleapis_com_data_access',
    }

    create_view = BigQueryOperator(
        dag=dag,
        task_id="cost_monitoring_view",
        sql='cost_monitoring.sql',
        params=params,
        use_legacy_sql=False,
        on_failure_callback=alerts.setup_callback(),
    )

    user_alerting = CostMonitoringOperator(
        config=config,
        dag=dag,
        task_id="cost_monitoring_users",
        sql="user_monitoring.sql",
        params=params,
        execution_timeout=timedelta(minutes=5),
        on_failure_callback=alerts.setup_callback(),
    )

    service_account_alerting = CostMonitoringOperator(
        config=config,
        dag=dag,
        task_id="cost_monitoring_service_accounts",
        sql="service_account_monitoring.sql",
        params=params,
        execution_timeout=timedelta(minutes=5),
        on_failure_callback=alerts.setup_callback(),
    )

    start >> create_view >> [user_alerting, service_account_alerting]
