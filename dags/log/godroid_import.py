import json
from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

import dwh_import
from configuration import config
from datahub.common import alerts
from datahub.dwh_import_read_replica import DwhImportReadReplica
from datahub.operators.delete_file_google_cloud_storage_operator import DeleteFileToGoogleCloudStorageOperator
from operators.godroid_to_google_cloud_storage import GodroidToGoogleCloudStorage

dwh_import_read_replica = DwhImportReadReplica(config)

version = 1

default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2019, 12, 12, 0, 0, 0),
    'retries': 2,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

doc_md = "#### Export godroid device information and import to BigQuery."


def get_schema_filename():
    return '{}/godroid/schema.json'.format(configuration.get('core', 'dags_folder'))


with DAG(
        dag_id=f'godroid-import-v{version}',
        description="Exports godroid device information to gcs and import to BigQuery.",
        schedule_interval='0 0 * * *',
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        tags=[dwh_import.DEFAULT_ARGS['owner'], 'dwh'],
        catchup=False) as dag:
    dag.doc_md = doc_md

    start = DummyOperator(task_id='start')

    with open(get_schema_filename()) as f:
        schema = json.load(f)

    dataset = config.get('bigquery').get('dataset').get('raw')
    gcs_bucket_name = config.get('go_droid_api').get('bucket_name')
    for region, endpoint in config.get('go_droid_api').get('regions').items():
        gcs_object_name = f'godroid/report/{{{{ ts_nodash }}}}/{region}.json'
        godroidtogcs_hook = GodroidToGoogleCloudStorage(
            region=region,
            endpoint=endpoint,
            task_id=f'godroid-import-to-gcs-{region}',
            execution_date="{{ ts }}",
            gcs_bucket_name=gcs_bucket_name,
            gcs_object_name=gcs_object_name,
            execution_timeout=timedelta(minutes=30),
            executor_config={
                'KubernetesExecutor': {
                    'request_memory': "2000Mi",
                    'limit_memory': "2000Mi",
                },
            },
            on_failure_callback=alerts.setup_callback(),
        )

        for database in dwh_import_read_replica.databases.values():
            if database.name == 'go_droid_api':
                for table in database.tables:
                    gcs_to_big_query = GoogleCloudStorageToBigQueryOperator(
                        task_id=f'godroid-gcs-to-bq-{region}',
                        bucket=gcs_bucket_name,
                        source_objects=[gcs_object_name],
                        source_format='NEWLINE_DELIMITED_JSON',
                        destination_project_dataset_table=f'{dataset}.{table.table_name}',
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
            task_id=f'godroid-delete-gcs-export-file-{region}',
            bucket=gcs_bucket_name,
            filename=gcs_object_name,
            on_failure_callback=alerts.setup_callback(),
        )

        start >> godroidtogcs_hook >> gcs_to_big_query >> delete_file
