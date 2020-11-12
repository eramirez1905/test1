from datetime import datetime, timedelta

from airflow import DAG

from configuration import config
from configuration.default_params import DAG_DEFAULT_ARGS
from datahub.dwh_import_read_replica import DwhImportReadReplica
from datahub.dwh_import_read_replica.dwh_import_read_replica_backfill import DwhImportReadReplicaBackfill

version = 1

dwh_import_read_replica = DwhImportReadReplica(config)

default_args = {
    # TODO Adjust based on your needs
    'start_date': datetime(2015, 1, 1),
    'retries': 3,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    dag_id=f'import-dwh-backfill-raw-layer-v{version}',
    description=f'Backfill the Raw Layer',
    schedule_interval=None,
    default_args={**DAG_DEFAULT_ARGS, **default_args},
    max_active_runs=1,
    concurrency=10,
    catchup=True,
    tags=[DAG_DEFAULT_ARGS['owner'], 'dwh-import', 'dwh'],
)

dag.doc_md = f"""
### Backfill the Raw Layer

This DAG triggers the Spark job in Databricks to *backfill* the data from read replicas RDS and store it into BigQuery

It is possible to filter the backfill by countries or regions, to do so please update in the configuration
(Variable *configuration*) the configuration `raw_layer_backfill`
"""

dwh_backfill = DwhImportReadReplicaBackfill(config=config)
dwh_backfill.import_from_read_replicas_backfill(dag)
