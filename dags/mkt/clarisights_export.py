from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator

import mkt_import
import clarisights_export
from configuration import config
from datahub.operators.redshift.redshift_create_table_operator import (
    RedshiftCreateTableOperator,
)
from datahub.operators.redshift.redshift_truncate_operator import (
    RedshiftTruncateOperator,
)
from datahub.operators.redshift.s3_to_redshift_iam_operator import (
    S3ToRedshiftIamOperator,
)


version = 1
doc_md = "#### Export CS unified marketing view to DWH"
default_args = {
    "start_date": datetime(2020, 10, 7, 0, 0, 0),
    "concurrency": 1,
}

CS_EXPORT_CONF = config.get("clarisights_exports")["campaign_costs"]
BQ_CONF = config.get("bigquery")

with DAG(
    dag_id=f"clarisights-export-to-dwh-v{version}",
    description="Export unified marketing CS view to DWH",
    default_args={**mkt_import.DEFAULT_ARGS, **default_args},
    tags=[mkt_import.DEFAULT_ARGS['owner'], "clarisights"],
    catchup=False,
    template_searchpath=clarisights_export.__path__,
    schedule_interval="0 4 * * *",
) as dag:
    dag.doc_md = doc_md
    #  Bigquery conf
    bq_conn_id = BQ_CONF.get("bigquery_conn_id_flat", "bigquery_default")
    project_id = BQ_CONF.get("project_id")
    staging_dataset = BQ_CONF.get("dataset").get("staging")
    staging_table = (
        f"{project_id}.{staging_dataset}.cs_campaign_costs_export_{{{{ ts_nodash }}}}"
    )
    #  GCS conf
    gcs_bucket = BQ_CONF.get("bucket_name")
    gcs_prefix = f"mkt/{{{{ ts_nodash }}}}/cs_campaign_costs_"
    staging_gcs_path = f"gs://{gcs_bucket}/{gcs_prefix}_*"
    #  S3 conf
    aws_conn_id = CS_EXPORT_CONF["aws_conn_id"]
    s3_bucket = CS_EXPORT_CONF["s3_bucket"]
    s3_prefix = CS_EXPORT_CONF["s3_prefix"]
    #  Redshift conf
    rs_conn_id = CS_EXPORT_CONF["redshift_conn_id"]
    rs_schema = CS_EXPORT_CONF["redshift_schema"]
    rs_table_name = CS_EXPORT_CONF["redshift_table_name"]

    start = DummyOperator(task_id="start")

    copy_campaign_costs_to_staging_table = BigQueryOperator(
        task_id="copy_campaign_costs_to_staging_table",
        sql=str(Path("sql") / "copy_campaign_costs_to_staging_table.sql"),
        use_legacy_sql=False,
        write_disposition="WRITE_TRUNCATE",
        destination_dataset_table=staging_table,
        bigquery_conn_id=bq_conn_id,
    )

    export_campaign_costs_to_gcs = BigQueryToCloudStorageOperator(
        task_id="export_campaign_costs_to_gcs",
        source_project_dataset_table=staging_table,
        destination_cloud_storage_uris=[staging_gcs_path],
        export_format="AVRO",
        bigquery_conn_id=bq_conn_id,
    )

    export_campaign_costs_from_gcs_to_s3 = GoogleCloudStorageToS3Operator(
        task_id="export_campaign_costs_from_gcs_to_s3",
        bucket=gcs_bucket,
        prefix=gcs_prefix,
        google_cloud_storage_conn_id=bq_conn_id,
        dest_aws_conn_id=aws_conn_id,
        dest_s3_key=f"s3://{s3_bucket}/{s3_prefix}/",
        replace=True,
        execution_timeout=timedelta(minutes=30),
        executor_config={
            "KubernetesExecutor": {
                "request_cpu": "200m",
                "limit_cpu": "400m",
                "request_memory": "4000Mi",
                "limit_memory": "4000Mi",
            }
        },
    )

    campaign_costs_table_columns = [
        ("cost_source", "VARCHAR(256)"),
        ("source_id", "SMALLINT"),
        ("entity_code", "VARCHAR(25)"),
        ("campaign", "VARCHAR(256)"),
        ("platform_type", "VARCHAR(12)"),
        ("platform", "VARCHAR(24)"),
        ("date", "TIMESTAMP"),
        ("cost_eur", "NUMERIC(15, 6)"),
    ]

    create_campaign_costs_table = RedshiftCreateTableOperator(
        task_id="create_campaign_costs_table",
        schema=rs_schema,
        table_name=rs_table_name,
        columns=campaign_costs_table_columns,
        diststyle="KEY",
        distkey="campaign",
        entity_to_privilege_grant={'GROUP "mkt_prd"': "ALL", "qa": "SELECT"},
        redshift_conn_id=rs_conn_id,
    )

    truncate_campaign_costs_table = RedshiftTruncateOperator(
        task_id="truncate_campaign_costs_table",
        schema=rs_schema,
        table=rs_table_name,
        redshift_conn_id=rs_conn_id,
    )

    import_campaign_costs_data_from_s3 = S3ToRedshiftIamOperator(
        task_id="import_campaign_costs_data_from_s3",
        schema=rs_schema,
        table=rs_table_name,
        s3_bucket=s3_bucket,
        s3_key=f"{s3_prefix}/{gcs_prefix}",
        redshift_conn_id=rs_conn_id,
        copy_options=("FORMAT AS AVRO 'auto'",),
    )

    (
        start
        >> copy_campaign_costs_to_staging_table
        >> export_campaign_costs_to_gcs
        >> export_campaign_costs_from_gcs_to_s3
        >> create_campaign_costs_table
        >> truncate_campaign_costs_table
        >> import_campaign_costs_data_from_s3
    )
