"""
Updates user defined functions (UDFs)
"""
from pathlib import Path

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from configs import CONFIG
from constants.airflow import DEFAULT_ARGS, GOOGLE_CLOUD_CONN
from transform.intermediate.udfs.constants import UDF_FILEPATHS
from utils.file import read_sql


with DAG(
    "update_persistent_udfs__intermediate_v1",
    default_args=DEFAULT_ARGS,
    schedule_interval=CONFIG.airflow.dags.raw.schedule,
    max_active_runs=1,
    catchup=False,
    tags=["transform", "intermediate"],
    doc_md=__doc__,
) as dag:

    for fp in UDF_FILEPATHS:
        udf = Path(fp).stem

        BigQueryOperator(
            task_id=f"update__{udf}",
            bigquery_conn_id=GOOGLE_CLOUD_CONN,
            sql=read_sql(Path(fp)),
            use_legacy_sql=False,
        )
