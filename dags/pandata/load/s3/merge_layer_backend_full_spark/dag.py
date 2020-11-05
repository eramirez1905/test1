"""
Syncs S3 to GCS bucket.

Then, loads tables from merge layer backend and does
minor transformations to clean invalid data with spark - full load
"""
from airflow import DAG
from airflow.contrib.operators.gcp_transfer_operator import (
    S3ToGoogleCloudStorageTransferOperator,
)
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator,
)
from airflow.operators.python_operator import PythonOperator

from configs import CONFIG
from constants.airflow import DEFAULT_ARGS, GOOGLE_CLOUD_CONN, AWS_CONN
from configs.bigquery.datasets.constants import (
    MERGE_LAYER_BACKEND_DATASET,
    MERGE_LAYER_BACKEND_LATEST_DATASET,
)
from configs.bigquery.tables.raw.pandora import PandoraDataProcBigQueryTable
from load.s3.constants import (
    DATAPROC_IDLE_DELETE_TTL,
    DATAPROC_IMAGE_VERSION,
    DATAPROC_LOCATION,
    DATAPROC_PROPERTIES,
    S3_BUCKET_PANDORA,
    ML_BE_AP_DIR,
)
from load.s3.merge_layer_backend_full_spark.constants import (
    DATAPROC_NAME,
    SPARK_BIGQUERY_CONNECTOR,
)
from load.s3.utils.bigquery import copy_table

with DAG(
    "load_merge_layer_backend_full_spark_v1",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=CONFIG.airflow.dags.raw__off_peak.schedule,
    tags=["load", "s3"],
    doc_md=__doc__,
) as dag:

    LOAD_DATASET = CONFIG.gcp.bigquery.get(MERGE_LAYER_BACKEND_DATASET)
    FINAL_DATASET = CONFIG.gcp.bigquery.get(MERGE_LAYER_BACKEND_LATEST_DATASET)
    TABLE_NAMES = [
        t.name
        for t in FINAL_DATASET.tables
        if t.is_full_load and isinstance(t, PandoraDataProcBigQueryTable)
    ]
    create_dataproc_cluster = DataprocClusterCreateOperator(
        task_id="create_dataproc_cluster",
        project_id=CONFIG.gcp.project_billing,
        gcp_conn_id=GOOGLE_CLOUD_CONN,
        cluster_name=DATAPROC_NAME,
        num_workers=6,
        region=DATAPROC_LOCATION,
        master_machine_type="n1-highcpu-16",
        worker_machine_type="n1-highmem-8",
        image_version=DATAPROC_IMAGE_VERSION,
        idle_delete_ttl=DATAPROC_IDLE_DELETE_TTL,
        properties=DATAPROC_PROPERTIES,
    )

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
            "includePrefixes": [f"{ML_BE_AP_DIR}/{t}/" for t in TABLE_NAMES]
        },
    )

    delete_dataproc_cluster = DataprocClusterDeleteOperator(
        task_id="delete_dataproc_cluster",
        project_id=CONFIG.gcp.project_billing,
        gcp_conn_id=GOOGLE_CLOUD_CONN,
        cluster_name=DATAPROC_NAME,
        region=DATAPROC_LOCATION,
    )

    for t in TABLE_NAMES:
        load_table = f"{CONFIG.gcp.project}.{LOAD_DATASET.name}.{t}_{{{{ ds_nodash }}}}"
        final_table = f"{CONFIG.gcp.project}.{FINAL_DATASET.name}.{t}"
        preprocess_and_load = DataProcPySparkOperator(
            task_id=f"preprocess_and_load__{t}",
            gcp_conn_id=GOOGLE_CLOUD_CONN,
            cluster_name=DATAPROC_NAME,
            main=f"gs://{CONFIG.gcp.bucket_pyspark}/jobs/fix_invalid_dates.py",
            region=DATAPROC_LOCATION,
            pyfiles=[f"gs://{CONFIG.gcp.bucket_pyspark}/packages.zip"],
            arguments=[
                (
                    f"gs://{CONFIG.gcp.bucket_pandora_export}/{ML_BE_AP_DIR}/{t}/"
                    "data*.parquet"
                ),
                "PARQUET",
                load_table,
                CONFIG.gcp.bucket_pyspark_temp,
            ],
            dataproc_pyspark_jars=[SPARK_BIGQUERY_CONNECTOR],
        )

        update_table = PythonOperator(
            task_id=f"update__{t}",
            python_callable=copy_table,
            op_kwargs={
                "bigquery_conn_id": GOOGLE_CLOUD_CONN,
                "source_project_dataset_tables": load_table,
                "destination_project_dataset_table": final_table,
            },
        )

        [create_dataproc_cluster, sync_s3_to_gcs] >> preprocess_and_load >> update_table
        preprocess_and_load >> delete_dataproc_cluster
