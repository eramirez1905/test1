"""
Loads clarisights tables from S3 to GCS to BigQuery
"""
from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcp_transfer_operator import (
    S3ToGoogleCloudStorageTransferOperator,
)
from airflow.operators.python_operator import PythonOperator

from configs import CONFIG
from constants.airflow import DEFAULT_ARGS, GOOGLE_CLOUD_CONN, AWS_CONN_MKT_CS
from configs.bigquery.datasets.constants import (
    CLARISIGHTS_DATASET,
    CLARISIGHTS_LATEST_DATASET,
)
from load.s3.constants import CLARISIGHTS_DIR, S3_BUCKET_MKT_CS
from load.s3.utils.bigquery import copy_table

with DAG(
    "load_clarisights_v1",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=CONFIG.airflow.dags.raw.schedule,
    tags=["load", "s3"],
    doc_md=__doc__,
) as dag:

    sync_s3_to_gcs = S3ToGoogleCloudStorageTransferOperator(
        task_id="sync_s3_to_gcs",
        s3_bucket=S3_BUCKET_MKT_CS,
        gcs_bucket=CONFIG.gcp.bucket_mkt_cs_export,
        project_id=CONFIG.gcp.project_billing,
        aws_conn_id=AWS_CONN_MKT_CS,
        gcp_conn_id=GOOGLE_CLOUD_CONN,
        description=f"{dag.dag_id}__{{{{ ds_nodash }}}}",
        retries=0,
        timeout=30 * 60,
        transfer_options={
            "overwriteObjectsAlreadyExistingInSink": True,
            "deleteObjectsUniqueInSink": True,
            "deleteObjectsFromSourceAfterTransfer": False,
        },
        object_conditions={"includePrefixes": [f"{CLARISIGHTS_DIR}/"]},
    )

    LOAD_DATASET = CONFIG.gcp.bigquery.get(CLARISIGHTS_DATASET)
    FINAL_DATASET = CONFIG.gcp.bigquery.get(CLARISIGHTS_LATEST_DATASET)
    for t in FINAL_DATASET.tables:
        load_table = (
            f"{CONFIG.gcp.project}.{LOAD_DATASET.name}.{t.name}_{{{{ ds_nodash }}}}"
        )
        load_gcs_to_bigquery = GoogleCloudStorageToBigQueryOperator(
            task_id=f"load__{t.name}__to_bq",
            bucket=CONFIG.gcp.bucket_mkt_cs_export,
            source_objects=[f"{CLARISIGHTS_DIR}/partner={t.name}/*.parquet"],
            destination_project_dataset_table=load_table,
            source_format="PARQUET",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            bigquery_conn_id=GOOGLE_CLOUD_CONN,
            google_cloud_storage_conn_id=GOOGLE_CLOUD_CONN,
            ignore_unknown_values=True,
        )

        update_table = PythonOperator(
            task_id=f"update__{t.name}",
            python_callable=copy_table,
            op_kwargs={
                "bigquery_conn_id": GOOGLE_CLOUD_CONN,
                "source_project_dataset_tables": load_table,
                "destination_project_dataset_table": (
                    f"{CONFIG.gcp.project}.{FINAL_DATASET.name}.{t.name}"
                ),
            },
        )

        sync_s3_to_gcs >> load_gcs_to_bigquery >> update_table
