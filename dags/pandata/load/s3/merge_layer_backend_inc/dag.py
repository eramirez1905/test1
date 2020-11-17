"""
Load merge layer backend tables incrementally from S3 to GCS to BigQuery
"""
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.gcp_transfer_operator import (
    S3ToGoogleCloudStorageTransferOperator,
)

from configs import CONFIG
from constants.airflow import DEFAULT_ARGS, GOOGLE_CLOUD_CONN, AWS_CONN
from configs.bigquery.datasets.constants import (
    MERGE_LAYER_BACKEND_INC_DATASET,
    MERGE_LAYER_BACKEND_LATEST_DATASET,
)
from configs.bigquery.tables.raw.pandora import PandoraBigQueryTable
from load.s3.constants import SQL_UTILS_DIR, S3_BUCKET_PANDORA, ML_BE_AP_INC_DIR
from load.s3.merge_layer_backend_inc.branch import branch_if_objects_exist
from utils.file import read_file

with DAG(
    "load_merge_layer_backend_incremental_v1",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=CONFIG.airflow.dags.raw.schedule,
    tags=["load", "s3"],
    doc_md=__doc__,
) as dag:

    LOAD_DATASET = CONFIG.gcp.bigquery.get(MERGE_LAYER_BACKEND_INC_DATASET)
    FINAL_DATASET = CONFIG.gcp.bigquery.get(MERGE_LAYER_BACKEND_LATEST_DATASET)
    TABLE_NAMES = [
        t.name for t in FINAL_DATASET.tables if isinstance(t, PandoraBigQueryTable)
    ]
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
        object_conditions={
            "includePrefixes": [f"{ML_BE_AP_INC_DIR}/{t}/" for t in TABLE_NAMES]
        },
    )

    for t in TABLE_NAMES:
        id_col = "joker_offer_id, order_code" if t == "order_joker" else "id"
        incremental_table = f"{LOAD_DATASET.name}.{t}_{{{{ ds_nodash }}}}"
        full_incremental_table = f"{CONFIG.gcp.project}.{incremental_table}"
        full_final_table = f"{CONFIG.gcp.project}.{FINAL_DATASET.name}.{t}"

        load_task_id = f"load__{t}__to_bq"
        do_not_load_task_id = f"do_not_load__{t}"

        load_if_objects_exist = BranchPythonOperator(
            task_id=f"load_if_objects_exist__{t}",
            python_callable=branch_if_objects_exist,
            op_kwargs={
                "google_cloud_storage_conn_id": GOOGLE_CLOUD_CONN,
                "bucket": CONFIG.gcp.bucket_pandora_export,
                "prefix": f"{ML_BE_AP_INC_DIR}/{t}/",
                "true_branch": load_task_id,
                "false_branch": do_not_load_task_id,
            },
        )

        do_not_load = DummyOperator(task_id=do_not_load_task_id)

        load_gcs_to_bigquery = GoogleCloudStorageToBigQueryOperator(
            task_id=load_task_id,
            bucket=CONFIG.gcp.bucket_pandora_export,
            source_objects=[f"{ML_BE_AP_INC_DIR}/{t}/data*.parquet"],
            destination_project_dataset_table=full_incremental_table,
            source_format="PARQUET",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            bigquery_conn_id=GOOGLE_CLOUD_CONN,
            google_cloud_storage_conn_id=GOOGLE_CLOUD_CONN,
            ignore_unknown_values=True,
        )

        append_to_final_table = BigQueryOperator(
            task_id=f"append_to__{t}",
            bigquery_conn_id=GOOGLE_CLOUD_CONN,
            sql=f"SELECT * FROM {full_incremental_table}",
            use_legacy_sql=False,
            priority="BATCH",
            destination_dataset_table=full_final_table,
            schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
            write_disposition="WRITE_APPEND",
            allow_large_results=True,
        )

        update_final_table = BigQueryOperator(
            task_id=f"update__{t}",
            bigquery_conn_id=GOOGLE_CLOUD_CONN,
            sql=read_file(SQL_UTILS_DIR / "dedup.sql").format(
                id_col=id_col, table_id=full_final_table
            ),
            use_legacy_sql=False,
            priority="BATCH",
            destination_dataset_table=full_final_table,
            write_disposition="WRITE_TRUNCATE",
            allow_large_results=True,
        )

        sync_s3_to_gcs >> load_if_objects_exist >> [
            load_gcs_to_bigquery,
            do_not_load,
        ]
        load_gcs_to_bigquery >> append_to_final_table >> update_final_table
