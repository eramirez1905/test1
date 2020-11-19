"""
Remove old metadata that is not needed to ensure
that the database has sufficient free capacity
"""
from airflow import DAG
from airflow.models import SlaMiss, TaskInstance, XCom
from airflow.operators.python_operator import PythonOperator

from configs import CONFIG
from constants.airflow import DEFAULT_ARGS
from monitor.metadata.clean import delete_object

with DAG(
    "cleanup_metadata",
    default_args=DEFAULT_ARGS,
    schedule_interval=CONFIG.airflow.dags.daily.schedule,
    max_active_runs=1,
    catchup=False,
    tags=["monitor"],
    doc_md=__doc__,
) as dag:
    CLEAN_CONFIG = [
        {"object": SlaMiss, "max_entry_age_in_days": 30},
        {"object": TaskInstance, "max_entry_age_in_days": 30},
        {"object": XCom, "max_entry_age_in_days": 2},
    ]

    for conf in CLEAN_CONFIG:
        cleanup = PythonOperator(
            task_id=f"cleanup_{conf['object'].__name__}",
            python_callable=delete_object,
            op_kwargs={
                "object": conf["object"],
                "days_before": conf["max_entry_age_in_days"],
            },
        )
