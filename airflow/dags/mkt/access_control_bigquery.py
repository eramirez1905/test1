import os
from datetime import datetime, timedelta

from airflow import DAG

import mkt_import
from configuration import config
from datahub.access_control.dwh_access_control_bigquery import DwhAccessControlBigQuery

version = 1
dwh_access_control_bigquery = DwhAccessControlBigQuery(config)

default_args = {
    'start_date': datetime(2020, 9, 22),
    'retries': 2,
    'concurrency': 20,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=15)
}

template_search_path = '{}/src/datahub/access_control/sql'.format(os.getenv("AIRFLOW_HOME"))

dag = DAG(dag_id=f'access-control-bigquery-v{version}',
          description=f'Update dataset permissions on BigQuery for external ventures',
          schedule_interval='*/15 * * * *',
          default_args={**mkt_import.DEFAULT_ARGS, **default_args},
          max_active_runs=1,
          template_searchpath=template_search_path,
          tags=[mkt_import.DEFAULT_ARGS['owner']],
          catchup=False)

dwh_access_control_bigquery.render_acl_tasks(dag)
