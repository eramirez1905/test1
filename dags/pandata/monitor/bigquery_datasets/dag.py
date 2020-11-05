"""
Update BigQuery datasets and create them if they don't exist
"""
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryPatchDatasetOperator,
)

from configs import CONFIG
from constants.airflow import DEFAULT_ARGS, GOOGLE_CLOUD_CONN

with DAG(
    "update_bigquery_datasets",
    default_args=DEFAULT_ARGS,
    schedule_interval=CONFIG.airflow.dags.daily.schedule,
    max_active_runs=1,
    catchup=False,
    tags=["monitor"],
    doc_md=__doc__,
) as dag:

    for d in CONFIG.gcp.bigquery.datasets:
        create_dataset = (
            BigQueryCreateEmptyDatasetOperator(
                task_id=f"create__{d.name}",
                bigquery_conn_id=GOOGLE_CLOUD_CONN,
                project_id=CONFIG.gcp.project,
                dataset_id=d.name,
                dataset_reference=d.properties,
                retries=0,
                on_failure_callback=None,
            ),
        )

        patch_dataset = BigQueryPatchDatasetOperator(
            task_id=f"patch__{d.name}",
            gcp_conn_id=GOOGLE_CLOUD_CONN,
            project_id=CONFIG.gcp.project,
            dataset_id=d.name,
            dataset_resource=d.properties,
            trigger_rule="all_done",
        )

        create_dataset >> patch_dataset
