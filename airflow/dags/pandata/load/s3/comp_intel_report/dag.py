"""
Loads Competition Intelligence Report table from S3 to GCS to BigQuery
"""
from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcp_transfer_operator import (
    S3ToGoogleCloudStorageTransferOperator,
)
from airflow.operators.python_operator import PythonOperator

from configs import CONFIG
from constants.airflow import DEFAULT_ARGS, GOOGLE_CLOUD_CONN, AWS_CONN
from configs.bigquery.datasets.constants import (
    COMP_INTEL_REPORT_DATASET,
    COMP_INTEL_REPORT_LATEST_DATASET,
)
from load.s3.constants import S3_BUCKET_COMP_INTEL
from load.s3.utils.bigquery import copy_table

with DAG(
    "load_comp_intel_report_v1",
    default_args=DEFAULT_ARGS,
    catchup=False,
    schedule_interval=CONFIG.airflow.dags.raw.schedule,
    tags=["load", "s3"],
    doc_md=__doc__,
) as dag:

    sync_s3_to_gcs = S3ToGoogleCloudStorageTransferOperator(
        task_id="sync_s3_to_gcs",
        s3_bucket=S3_BUCKET_COMP_INTEL,
        gcs_bucket=CONFIG.gcp.bucket_comp_intel_export,
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
    )

    LOAD_DATASET = CONFIG.gcp.bigquery.get(COMP_INTEL_REPORT_DATASET)
    FINAL_DATASET = CONFIG.gcp.bigquery.get(COMP_INTEL_REPORT_LATEST_DATASET)

    TABLE_NAME = "competition_dashboard"
    LOAD_TABLE = (
        f"{CONFIG.gcp.project}.{LOAD_DATASET.name}.{TABLE_NAME}_{{{{ ds_nodash }}}}"
    )
    FINAL_TABLE = f"{CONFIG.gcp.project}.{FINAL_DATASET.name}.{TABLE_NAME}"

    load_gcs_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id=f"load__{TABLE_NAME}__to_bq",
        bucket=CONFIG.gcp.bucket_comp_intel_export,
        source_objects=["competition_dashboard/data000.gz"],
        compression="GZIP",
        destination_project_dataset_table=LOAD_TABLE,
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "region", "type": "STRING", "mode": "NULLABLE"},
            {"name": "competitor_google", "type": "STRING", "mode": "NULLABLE"},
            {"name": "source_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "is_dh_brand", "type": "STRING", "mode": "NULLABLE"},
            {"name": "brand_display_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "business_model", "type": "STRING", "mode": "NULLABLE"},
            {"name": "is_primary_competitor", "type": "STRING", "mode": "NULLABLE"},
            {"name": "url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "report_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "report_quarter", "type": "STRING", "mode": "NULLABLE"},
            {"name": "granularity", "type": "STRING", "mode": "NULLABLE"},
            {"name": "online_restaurants", "type": "INT64", "mode": "NULLABLE"},
            {"name": "online_restaurants_lm", "type": "INT64", "mode": "NULLABLE"},
            {"name": "online_restaurants_ly", "type": "INT64", "mode": "NULLABLE"},
            {"name": "queries_total", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "queries_total_mobile", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "queries_total_desktop", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "br_is_last_survey", "type": "STRING", "mode": "NULLABLE"},
            {"name": "br_total_survey_sample", "type": "INT64", "mode": "NULLABLE"},
            {"name": "br_unaided_awareness", "type": "INT64", "mode": "NULLABLE"},
            {"name": "br_aided_brand_awareness", "type": "INT64", "mode": "NULLABLE"},
            {"name": "br_brand_consideration", "type": "INT64", "mode": "NULLABLE"},
            {"name": "br_brand_usage", "type": "INT64", "mode": "NULLABLE"},
            {"name": "br_brand_preference", "type": "INT64", "mode": "NULLABLE"},
            {"name": "br_top_of_mind", "type": "INT64", "mode": "NULLABLE"},
            {"name": "sw_desktop_visits", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "sw_mobile_web_visits", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "sw_desktop_bounce_rate", "type": "FLOAT64", "mode": "NULLABLE"},
            {
                "name": "sw_mobile_web_bounce_rate",
                "type": "FLOAT64",
                "mode": "NULLABLE",
            },
            {
                "name": "sw_desktop_unique_visitors",
                "type": "FLOAT64",
                "mode": "NULLABLE",
            },
            {
                "name": "sw_mobile_web_unique_visitors",
                "type": "FLOAT64",
                "mode": "NULLABLE",
            },
            {
                "name": "appannie_ios_iphone_downloads",
                "type": "INT64",
                "mode": "NULLABLE",
            },
            {"name": "appannie_ios_all_downloads", "type": "INT64", "mode": "NULLABLE"},
            {"name": "appannie_and_all_downloads", "type": "INT64", "mode": "NULLABLE"},
            {
                "name": "appannie_install_penetration_ios",
                "type": "FLOAT64",
                "mode": "NULLABLE",
            },
            {"name": "appannie_active_users_ios", "type": "INT64", "mode": "NULLABLE"},
            {
                "name": "appannie_total_sessions_ios",
                "type": "INT64",
                "mode": "NULLABLE",
            },
            {
                "name": "appannie_install_penetration_and",
                "type": "FLOAT64",
                "mode": "NULLABLE",
            },
            {"name": "appannie_active_users_and", "type": "INT64", "mode": "NULLABLE"},
            {
                "name": "appannie_total_sessions_and",
                "type": "INT64",
                "mode": "NULLABLE",
            },
        ],
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        bigquery_conn_id=GOOGLE_CLOUD_CONN,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_CONN,
        ignore_unknown_values=True,
    )

    update_table = PythonOperator(
        task_id=f"update__{TABLE_NAME}",
        python_callable=copy_table,
        op_kwargs={
            "bigquery_conn_id": GOOGLE_CLOUD_CONN,
            "source_project_dataset_tables": LOAD_TABLE,
            "destination_project_dataset_table": FINAL_TABLE,
        },
    )

    sync_s3_to_gcs >> load_gcs_to_bigquery >> update_table
