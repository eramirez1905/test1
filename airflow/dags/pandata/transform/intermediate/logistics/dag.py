"""
Creates or updates views for intermediate layer for pandora data
"""
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from configs import CONFIG
from configs.bigquery.datasets.constants import INTERMEDIATE_DATASET
from configs.bigquery.tables.intermediate import LogisticsBigQueryView
from constants.airflow import DEFAULT_ARGS, GOOGLE_CLOUD_CONN
from transform.intermediate.logistics.constants import SQL_DIR
from transform.intermediate.utils import add_upsert_view_ddl
from utils.file import read_sql

with DAG(
    "update_intermediate_layer__logistics_v1",
    default_args=DEFAULT_ARGS,
    schedule_interval=CONFIG.airflow.dags.raw.schedule,
    max_active_runs=1,
    catchup=False,
    tags=["transform", "intermediate"],
    doc_md=__doc__,
) as dag:
    dataset = CONFIG.gcp.bigquery.get(INTERMEDIATE_DATASET)
    for t in dataset.tables:
        if isinstance(t, LogisticsBigQueryView):
            BigQueryOperator(
                task_id=f"update__{t.name}",
                bigquery_conn_id=GOOGLE_CLOUD_CONN,
                sql=add_upsert_view_ddl(
                    project_name=CONFIG.gcp.project,
                    dataset_name=dataset.name,
                    view_name=t.name,
                    sql=read_sql(SQL_DIR / f"{t.name}.sql"),
                ),
                use_legacy_sql=False,
            )
