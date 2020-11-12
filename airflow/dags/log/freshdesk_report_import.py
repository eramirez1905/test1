from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

import dwh_import
from configuration import config
from datahub.common import alerts
from datahub.dwh_import_read_replica import DwhImportReadReplica
from datahub.operators.freshdesk_report_to_google_cloud_storage_operator import FreshdeskReportToGoogleCloudStorageOperator
from freshdesk import schema

dwh_import_read_replica = DwhImportReadReplica(config)

version = 3

default_args = {
    'owner': 'freshdesk-reports',
    'start_date': datetime(2019, 12, 2, 0, 0, 0),
    'retries': 2,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

doc_md = f"""
#### Export reports from FreshDesk and import to BigQuery
"""

with DAG(
        dag_id=f'freshdesk-report-import-v{version}',
        description=f'Export reports from FreshDesk and import to BigQuery',
        schedule_interval='0 7 * * *',
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        max_active_runs=1,
        tags=[dwh_import.DEFAULT_ARGS['owner'], 'dwh'],
        catchup=False) as dag:
    dag.doc_md = doc_md

    start = DummyOperator(
        task_id='start'
    )

    dataset = config.get('bigquery').get('dataset').get('raw')
    bucket_name = config.get('freshdesk').get('bucket_name')

    for region, report in config.get('freshdesk').get('reports').items():

        gcs_object_name = f'freshdesk_export/report_{region}_{{{{ ds }}}}.json'

        fresh_desk_hook = FreshdeskReportToGoogleCloudStorageOperator(
            task_id=f'upload-to-gcs-{region}',
            region=region,
            dst=gcs_object_name,
            bucket=bucket_name,
            http_conn_id=f'freshdesk_{region}',
            request_params={
                "created_at": "{{ next_ds }}"
            },
            endpoint=report['url'],
            mime_type='text/json',
            execution_timeout=timedelta(minutes=30),
            on_failure_callback=alerts.setup_callback(),
        )

        for database in dwh_import_read_replica.databases.values():
            if database.name == 'freshdesk':
                for table in database.tables:
                    gcs_to_big_query = GoogleCloudStorageToBigQueryOperator(
                        task_id=f'freshdesk-gcs-to-bq-{region}',
                        bucket=bucket_name,
                        source_objects=[gcs_object_name],
                        source_format='NEWLINE_DELIMITED_JSON',
                        destination_project_dataset_table=f'{dataset}.freshdesk_reports',
                        schema_fields=schema,
                        skip_leading_rows=0,
                        time_partitioning=dwh_import_read_replica.get_time_partitioning_column(table),
                        cluster_fields=dwh_import_read_replica.get_cluster_fields(table),
                        create_disposition='CREATE_IF_NEEDED',
                        write_disposition='WRITE_APPEND',
                        schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
                        execution_timeout=timedelta(minutes=30),
                        on_failure_callback=alerts.setup_callback(),
                    )

        start >> fresh_desk_hook >> gcs_to_big_query
