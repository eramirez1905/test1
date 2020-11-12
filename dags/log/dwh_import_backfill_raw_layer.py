import os
from datetime import datetime, timedelta

from airflow import DAG

import dwh_import
from configuration import config
from datahub.dwh_import_read_replica import DwhImportReadReplica
from datahub.dwh_import_read_replica.dwh_import_read_replica_backfill import DwhImportReadReplicaBackfill

version = 1

dwh_import_read_replica = DwhImportReadReplica(config)

default_args = {
    'start_date': datetime(2015, 1, 1),
    'retries': 3,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=10),
}

template_search_path = '{}/src/datahub/dwh_import_read_replica/sql'.format(os.getenv("AIRFLOW_HOME"))

dag = DAG(
    dag_id=f'import-dwh-backfill-raw-layer-v{version}',
    description=f'Backfill the Raw Layer',
    schedule_interval=None,
    default_args={**dwh_import.DEFAULT_ARGS, **default_args},
    max_active_runs=1,
    concurrency=10,
    template_searchpath=template_search_path,
    catchup=True,
    tags=[dwh_import.DEFAULT_ARGS['owner'], 'dwh-import', 'dwh'],
)

dag.doc_md = f"""
### Backfill the Raw Layer

This DAG triggers the Spark job in Databricks to *backfill* the data from read replicas RDS and store it into BigQuery

It is possible to filter the backfill by countries or regions, to do so please update in the configuration
(Variable *configuration*) the configuration `raw_layer_backfill`
"""

dwh_backfill = DwhImportReadReplicaBackfill(config=config)
dwh_backfill.import_from_read_replicas_backfill(dag)
