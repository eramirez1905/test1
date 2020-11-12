from datetime import datetime, timedelta

import mkt_import
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.delete_synced_s3_objects_operator import DeleteSyncedS3Objects
from operators.sync_s3_bucket_across_accounts_operator import SyncS3BucketAcrossAccounts

from configuration import config

version = 1
default_args = {
    'owner': 'mkt-ads',
    'start_date': datetime(2020, 6, 9, 0, 0, 0),
    'retries': 2,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

doc_md = "#### Process the raw data exports from Adjust to S3"


with DAG(
        dag_id=f'process-adjust-exports-v{version}',
        description='Process the raw data exports from Adjust to S3',
        default_args={**mkt_import.DEFAULT_ARGS, **default_args},
        catchup=False,
        schedule_interval='30 * * * *'
) as dag:
    dag.doc_md = doc_md

    mkt_adjust_exports_bucket = config['adjust_exports']['s3']['mkt_exports_bucket']
    mkt_adjust_archive_bucket = config['adjust_exports']['s3']['mkt_archive_bucket']
    dwh_adjust_exports_bucket = config['adjust_exports']['s3']['dwh_exports_bucket']
    dwh_adjust_exports_prefix = config['adjust_exports']['s3']['dwh_exports_prefix']
    connection_id = config['adjust_exports']['s3']['aws_conn_id']

    start = DummyOperator(task_id='start')

    copy_s3_files_to_dwh = SyncS3BucketAcrossAccounts(
        aws_conn_id=connection_id,
        source_bucket_name=mkt_adjust_exports_bucket,
        dest_bucket_name=dwh_adjust_exports_bucket,
        dest_prefix=dwh_adjust_exports_prefix,
        task_id='copy_s3_files_to_dwh_adjust',
    )

    stream_to_bq = DummyOperator(task_id='todo_stream_to_bq')

    copy_s3_files_to_archive_bucket = SyncS3BucketAcrossAccounts(
        aws_conn_id=connection_id,
        source_bucket_name=mkt_adjust_exports_bucket,
        dest_bucket_name=mkt_adjust_archive_bucket,
        task_id='copy_s3_files_to_mkt_archive',
    )

    empty_mkt_adjust_bucket = DeleteSyncedS3Objects(
        aws_conn_id=connection_id,
        bucket_name=mkt_adjust_exports_bucket,
        task_id='empty_mkt_adjust_bucket',
        object_keys="{{ task_instance.xcom_pull(task_ids='copy_s3_files_to_mkt_archive', key='return_value') }}"
    )

    start >> copy_s3_files_to_dwh >> stream_to_bq >> copy_s3_files_to_archive_bucket >> empty_mkt_adjust_bucket
