from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.operators.dummy_operator import DummyOperator

import dwh_import
from configuration import config
from datahub.common import alerts
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator

template_search_path = '{}/pandora_view/sql'.format(configuration.get('core', 'dags_folder'))
version = 1
default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2020, 4, 17, 0, 0, 0),
    'retries': 3,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=1),
}

doc_md = "#### Dag that creates a view for pandora. The view is on top of the rl layer. "

with DAG(
        dag_id=f'pandora-view-v{version}',
        description='DAG which creates views on top of rl for pandora access.',
        schedule_interval='0 12 * * *',
        template_searchpath=template_search_path,
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        tags=[dwh_import.DEFAULT_ARGS['owner'], 'dwh'],
        catchup=False) as dag:
    dag.doc_md = doc_md

    start = DummyOperator(
        task_id='start',
        on_failure_callback=alerts.setup_callback(),
    )

    """
    We are only sharing payment_report with pandora in the special dataset.
    """
    params = {
        'project_id': config.get('bigquery').get('project_id'),
        'table_name': 'payment_report',
    }

    create_view = BigQueryOperator(
        dag=dag,
        task_id="create-view",
        sql='pandora_dh_view.sql',
        params=params,
        use_legacy_sql=False,
        on_failure_callback=alerts.setup_callback(),
    )

    start >> create_view
