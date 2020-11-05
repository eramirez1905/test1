"""
Updates intermediate table incrementally.
To backfill, increment the version number before activating this DAG.

When the schedule is changed, increment the version number and
change the start date.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from configs import CONFIG
from configs.bigquery.datasets.constants import INTERMEDIATE_DATASET
from configs.bigquery.tables.intermediate import VciBigQueryIncTable
from constants.airflow import DEFAULT_ARGS, GOOGLE_CLOUD_CONN
from transform.intermediate.vci.inc.constants import SQL_DIR
from utils.file import read_sql

with DAG(
    "update_intermediate_layer_incremental__vci_v2",
    default_args=DEFAULT_ARGS,
    schedule_interval=CONFIG.airflow.dags.daily.schedule,
    max_active_runs=10,
    catchup=True,
    tags=["transform", "intermediate", "incremental"],
    start_date=datetime(2020, 9, 14),  # earliest created_date
    doc_md=__doc__,
) as dag:
    dataset = CONFIG.gcp.bigquery.get(INTERMEDIATE_DATASET)
    for t in dataset.tables:
        if isinstance(t, VciBigQueryIncTable):
            update_table = BigQueryOperator(
                task_id=f"update__{t.name}",
                bigquery_conn_id=GOOGLE_CLOUD_CONN,
                destination_dataset_table=f"{CONFIG.gcp.project}.{dataset.name}.{t.name}${{{{ ds_nodash }}}}",
                sql=read_sql(SQL_DIR / f"{t.name}.sql", created_date="{{ ds }}"),
                priority="BATCH",
                write_disposition="WRITE_TRUNCATE",
                time_partitioning=t.time_partitioning,
                use_legacy_sql=False,
                execution_timeout=timedelta(minutes=30),
            )
