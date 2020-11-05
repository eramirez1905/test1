import datetime

import mkt_import
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator


version = 1
default_args = {
    'start_date': datetime.datetime(2020, 6, 10, 0, 0, 0),
    'concurrency': 1,
    'retries': 0,
    'max_active_runs': 1,
}


with DAG(
        dag_id=f'dwh-redshift-ping-v{version}',
        description='Test connection to central dwh redshift',
        default_args={**mkt_import.DEFAULT_ARGS, **default_args},
        catchup=False,
        schedule_interval=None
) as dag:
    rs_test = PostgresOperator(
        task_id='select_dim_countries',
        postgres_conn_id='redshift_dwh',
        sql='select * from dwh_il.dim_countries'
    )
