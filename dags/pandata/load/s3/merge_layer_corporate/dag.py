"""
Loads merge layer corporate tables from S3 to GCS to BigQuery
"""
from airflow import DAG
from airflow.contrib.operators.gcp_transfer_operator import (
    S3ToGoogleCloudStorageTransferOperator,
)

from configs import CONFIG
from constants.airflow import DEFAULT_ARGS, GOOGLE_CLOUD_CONN, AWS_CONN
from configs.bigquery.datasets.constants import (
    MERGE_LAYER_CORPORATE_DATASET,
    MERGE_LAYER_CORPORATE_LATEST_DATASET,
)
from load.s3.constants import S3_BUCKET_PANDORA, ML_CP_AP_DIR
from load.s3.utils.tasks import load_table_and_update

with DAG(
    "load_merge_layer_corporate_v1",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=CONFIG.airflow.dags.raw.schedule,
    tags=["load", "s3"],
    doc_md=__doc__,
) as dag:

    sync_s3_to_gcs = S3ToGoogleCloudStorageTransferOperator(
        task_id="sync_s3_to_gcs",
        s3_bucket=S3_BUCKET_PANDORA,
        gcs_bucket=CONFIG.gcp.bucket_pandora_export,
        project_id=CONFIG.gcp.project_billing,
        aws_conn_id=AWS_CONN,
        gcp_conn_id=GOOGLE_CLOUD_CONN,
        description=f"{dag.dag_id}__{{{{ ds_nodash }}}}",
        retries=0,
        timeout=30 * 60,
        transfer_options={
            "overwriteObjectsAlreadyExistingInSink": True,
            "deleteObjectsUniqueInSink": True,
            "deleteObjectsFromSourceAfterTransfer": False,
        },
        object_conditions={"includePrefixes": [f"{ML_CP_AP_DIR}/"]},
    )

    LOAD_DATASET = CONFIG.gcp.bigquery.get(MERGE_LAYER_CORPORATE_DATASET)
    FINAL_DATASET = CONFIG.gcp.bigquery.get(MERGE_LAYER_CORPORATE_LATEST_DATASET)
    for t in FINAL_DATASET.tables:
        load_table_and_update(
            dir_name=ML_CP_AP_DIR,
            table_id=t.name,
            load_tables_dataset_id=LOAD_DATASET.name,
            final_tables_dataset_id=FINAL_DATASET.name,
            project_id=CONFIG.gcp.project,
            bigquery_conn_id=GOOGLE_CLOUD_CONN,
            key_prefix="data",
            bucket_name=CONFIG.gcp.bucket_pandora_export,
            upstream_task=sync_s3_to_gcs,
            dag=dag,
        )
