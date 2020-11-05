import json
from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

import dwh_import
from configuration import config
from datahub.common import alerts
from datahub.common.helpers import DatabaseSettings
from datahub.dwh_import_read_replica import DwhImportReadReplica
from datahub.operators.delete_file_google_cloud_storage_operator import DeleteFileToGoogleCloudStorageOperator
from operators.soti_to_google_cloud_storage import SotiToGoogleCloudStorage

dwh_import_read_replica = DwhImportReadReplica(config)

version = 1

default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2019, 9, 27, 0, 0, 0),
    'retries': 2,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=30),
}

doc_md = f"""
#### Export SOTI device information and import to BigQuery.
"""


def get_schema_filename():
    return '{}/soti/schema.json'.format(configuration.get('core', 'dags_folder'))


with DAG(
        dag_id=f'soti-import-v{version}',
        description=f'Exports SOTI device information and import to BigQuery.',
        schedule_interval='30 * * * *',
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        tags=[dwh_import.DEFAULT_ARGS['owner'], 'dwh'],
        catchup=False) as dag:
    dag.doc_md = doc_md

    start = DummyOperator(
        task_id='start'
    )

    dataset = config.get('bigquery').get('dataset').get('raw')
    gcs_bucket_name = config.get('soti').get('bucket_name')
    for region in config.get('soti').get('regions'):
        with open(get_schema_filename()) as f:
            schema = json.load(f)

        gcs_object_name = f'soti_export/report_{region}_{{{{ ts_nodash }}}}.json'
        soti_hook = SotiToGoogleCloudStorage(
            region=region,
            task_id=f'soti-import-to-gcs-{region}',
            soti_conn_id=f'soti_{region}',
            execution_date="{{ts}}",
            gcs_bucket_name=gcs_bucket_name,
            gcs_object_name=gcs_object_name,
            execution_timeout=timedelta(minutes=60),
            executor_config={
                'KubernetesExecutor': {
                    'request_memory': "20000Mi",
                    'limit_memory': "20000Mi",
                    'node_selectors': config.get('soti').get('node_selectors'),
                    'tolerations': config.get('soti').get('tolerations'),
                },
            },
            on_failure_callback=alerts.setup_callback(),
        )

        for database_name, params in config['dwh_merge_layer_databases'].items():
            if database_name == 'soti':
                database = DatabaseSettings(database_name, params)
                for table in database.tables:
                    gcs_to_big_query = GoogleCloudStorageToBigQueryOperator(
                        task_id=f'soti-gcs-to-bq-{region}',
                        bucket=gcs_bucket_name,
                        source_objects=[gcs_object_name],
                        source_format='NEWLINE_DELIMITED_JSON',
                        destination_project_dataset_table=f'{dataset}.soti_devices',
                        schema_fields=schema,
                        skip_leading_rows=0,
                        time_partitioning=dwh_import_read_replica.get_time_partitioning_column(table),
                        cluster_fields=dwh_import_read_replica.get_cluster_fields(table),
                        create_disposition='CREATE_IF_NEEDED',
                        write_disposition='WRITE_APPEND',
                        schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
                        execution_timeout=timedelta(minutes=30),
                        on_failure_callback=alerts.setup_callback(),
                        max_bad_records=0,
                    )

        delete_file = DeleteFileToGoogleCloudStorageOperator(
            task_id=f'soti-delete-gcs-export-file-{region}',
            bucket=gcs_bucket_name,
            filename=gcs_object_name,
            on_failure_callback=alerts.setup_callback(),
        )

        start >> soti_hook >> gcs_to_big_query >> delete_file
