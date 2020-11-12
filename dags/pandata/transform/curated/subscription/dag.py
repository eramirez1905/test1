"""
Transforms data from subscription merge layer and output
tables in `pandata_curated` dataset
"""
from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from configs import CONFIG
from configs.bigquery.datasets.constants import CURATED_DATASET
from configs.bigquery.tables.curated import SubscriptionTable
from constants.airflow import DEFAULT_ARGS, GOOGLE_CLOUD_CONN
from transform.curated.subscription.constants import SQL_DIR
from utils.file import read_sql
from utils.operators.bigquery import BigQueryPatchTableOperator

with DAG(
    "update_curated_layer__subscription_v1",
    default_args=DEFAULT_ARGS,
    schedule_interval=CONFIG.airflow.dags.curated.schedule,
    max_active_runs=1,
    catchup=False,
    tags=["transform", "curated"],
    doc_md=__doc__,
) as dag:

    DATASET = CONFIG.gcp.bigquery.get(CURATED_DATASET)

    for t in DATASET.tables:
        if isinstance(t, SubscriptionTable):
            update_table = BigQueryOperator(
                task_id=f"update__{t.name}",
                bigquery_conn_id=GOOGLE_CLOUD_CONN,
                sql=read_sql(SQL_DIR / f"{t.name}.sql"),
                destination_dataset_table=f"{CONFIG.gcp.project}.{DATASET.name}.{t.name}",
                write_disposition="WRITE_TRUNCATE",
                execution_timeout=timedelta(minutes=30),
                priority="BATCH",
                use_legacy_sql=False,
                time_partitioning=t.time_partitioning,
                cluster_fields=t.cluster_fields,
            )

            patch_table = BigQueryPatchTableOperator(
                task_id=f"patch__{t.name}",
                gcp_conn_id=GOOGLE_CLOUD_CONN,
                project_id=CONFIG.gcp.project,
                dataset_id=DATASET.name,
                table_id=t.name,
                description=t.description,
                fields=t.fields,
                is_partition_filter_required=t.is_partition_filter_required,
            )

            update_table >> patch_table
