from datetime import datetime, timedelta

from airflow import DAG

import dwh_import
from configuration import config
from datahub.dwh_import_read_replica import DwhImportReadReplica
from salesforce.salesforce_import import SalesforceImport

version = 1

default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2020, 1, 29, 0, 0, 0),
    'retries': 0,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

doc_md = "#### Backfill Salesforce data and import to BigQuery."

with DAG(
        dag_id=f'salesforce-backfill-import-v{version}',
        description=f'Backfill Salesforce data and import to BigQuery.',
        schedule_interval=None,
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        tags=[dwh_import.DEFAULT_ARGS['owner'], 'dwh'],
        catchup=False) as dag:
    dag.doc_md = doc_md

    salesforce_import = SalesforceImport(
        backfill=True,
        dwh_import=DwhImportReadReplica(config),
    )
    salesforce_import.render()
