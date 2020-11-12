from datetime import datetime, timedelta

from airflow import DAG

import dwh_import
from cloud_health.cloud_health_import import CloudHealthImport
from configuration import config
from datahub.dwh_import_read_replica import DwhImportReadReplica

version = 1

default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2020, 8, 26, 0, 0, 0),
    'retries': 2,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=30),
}

doc_md = f"""
#### Backfill Cloud Health reports and import to BigQuery.

This DAG imports all the available days in the report.
"""

with DAG(
        dag_id=f'cloud-health-backfill-v{version}',
        description=f'Export cloud_health reports and import to BigQuery.',
        schedule_interval=None,
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        tags=[dwh_import.DEFAULT_ARGS['owner'], 'dwh']
) as dag:
    dag.doc_md = doc_md

    gcs_bucket_name = config.get('bigquery').get('bucket_name')
    project_id = config.get('bigquery').get('project_id')
    dataset_id = config.get('bigquery').get('dataset').get('raw')

    cloud_health_import = CloudHealthImport(
        dataset_id=dataset_id,
        project_id=project_id,
        gcs_bucket_name=gcs_bucket_name,
        is_backfill=True,
        dwh_import=DwhImportReadReplica(config)
    )

    cloud_health_import.render()
