from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator
from airflow.operators.dummy_operator import DummyOperator

import dwh_import
from configuration import config
from datahub.common import alerts
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from datahub.operators.delete_file_google_cloud_storage_operator import DeleteFileToGoogleCloudStorageOperator

template_search_path = '{}/road_runner/sql'.format(configuration.get('core', 'dags_folder'))
version = 1
default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2020, 3, 17, 0, 0, 0),
    'retries': 2,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

doc_md = "#### Export the heat map of past orders for riders."
GCS_FOLDER = 'road_runner'
STAGING_DATASET = 'staging'
STAGING_TABLE_NAME = 'roadrunner_heatmap'

with DAG(
        dag_id=f'roadrunner-export-daily-v{version}',
        description='Road runner export of heat map.',
        schedule_interval='0 12 * * *',
        template_searchpath=template_search_path,
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        tags=[dwh_import.DEFAULT_ARGS['owner'], 'dwh'],
        catchup=False) as dag:
    dag.doc_md = doc_md

    gcs_bucket = config.get('bigquery').get('bucket_name')
    gcs_file_name = f'{GCS_FOLDER}/heatmap_{{{{ yesterday_ds_nodash }}}}.csv'
    path = f'gs://{gcs_bucket}/{gcs_file_name}'
    staging_table = f"{STAGING_DATASET}.{STAGING_TABLE_NAME}_{{{{ ts_nodash }}}}"

    start = DummyOperator(task_id='start')

    store_in_staging = BigQueryOperator(
        task_id='road_runner_to_staging',
        sql='road_runner.sql',
        destination_dataset_table=staging_table,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        execution_timeout=timedelta(minutes=20),
        on_failure_callback=alerts.setup_callback(),
    )

    extract_to_gcs = BigQueryToCloudStorageOperator(
        task_id='extract_to_gcs_road_runner',
        source_project_dataset_table=staging_table,
        destination_cloud_storage_uris=[path],
        compression='GZIP',
        export_format='CSV',
        field_delimiter=',',
        execution_timeout=timedelta(minutes=30),
        on_failure_callback=alerts.setup_callback(),
    )

    s3_bucket = config.get('roadrunner').get('s3_bucket_name')
    gcs_to_s3 = GoogleCloudStorageToS3Operator(
        task_id='cp_gcs_road_runner_to_s3',
        google_cloud_storage_conn_id='bigquery_default',
        dest_aws_conn_id='aws_default',
        bucket=gcs_bucket,
        prefix=GCS_FOLDER,
        delimiter='.csv',
        dest_s3_key=s3_bucket,
        replace=True,
        execution_timeout=timedelta(minutes=30),
        on_failure_callback=alerts.setup_callback(),
    )

    delete_heat_map = DeleteFileToGoogleCloudStorageOperator(
        task_id='delete_road_runner_heat_map_from_gcs',
        bucket=gcs_bucket,
        filename=gcs_file_name,
        on_failure_callback=alerts.setup_callback(),
    )

    start >> store_in_staging >> extract_to_gcs >> gcs_to_s3 >> delete_heat_map
