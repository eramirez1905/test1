"""
Compare tables loaded with existing directories in S3.
Task will fail if there are new child directories.
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from configs import CONFIG
from constants.airflow import AWS_CONN, DEFAULT_ARGS
from monitor.s3_directories.check import check_expected_child_dirs
from monitor.s3_directories.constants import CHECKS
from utils.tasks import replace_non_alnum_chars

with DAG(
    "check_for_additional_s3_directories",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup=False,
    schedule_interval=CONFIG.airflow.dags.raw.schedule,
    tags=["monitor", "s3"],
    doc_md=__doc__,
) as dag:

    for check in CHECKS:
        tables_check = PythonOperator(
            task_id=f"check_tables__{replace_non_alnum_chars(check.parent_dir)}",
            python_callable=check_expected_child_dirs,
            op_kwargs={
                "aws_conn_id": AWS_CONN,
                "parent_dir": check.parent_dir,
                "expected_child_dirs": check.expected_child_dirs,
            },
        )
