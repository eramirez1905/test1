from datetime import datetime, timedelta

from airflow import DAG, conf

import mkt_import
from configuration import config
from datahub.curated_data.entities_config import EntitiesConfig
from datahub.curated_data.process_curated_data import ProcessCuratedData
from datahub.dwh_import_read_replica import DwhImportReadReplica

version = 2

default_args = {
    "owner": "mkt-data",
    "start_date": datetime(2020, 5, 6, 5, 0, 0),
    "retries": 2,
    "concurrency": 20,
    "max_active_runs": 1,
    "retry_delay": timedelta(minutes=5),
    "executor_config": {
        "KubernetesExecutor": {
            "request_cpu": "200m",
            "limit_cpu": "200m",
            "request_memory": "500Mi",
            "limit_memory": "500Mi",
        },
    },
}

doc_md = """
#### Create Curated Data on BigQuery
"""

dwh_import_read_replicas = DwhImportReadReplica(config)

with DAG(
    dag_id=f"mkt-ncr-curated-data-v{version}",
    description="Create Curated Data on BigQuery",
    schedule_interval="0 */4 * * *",
    default_args={**mkt_import.DEFAULT_ARGS, **default_args},
    max_active_runs=1,
    template_searchpath=f"{conf.get('core', 'dags_folder')}/curated_data/sql/",
    tags=[mkt_import.DEFAULT_ARGS["owner"], "curated-data"],
    catchup=False,
) as dag:
    dag.doc_md = doc_md

    etl = ProcessCuratedData(
        dag=dag,
        project_id=config.get("bigquery").get("project_id"),
        dataset_id=config.get("bigquery", {}).get("dataset", {}).get("cl", "cl_mkt"),
        config=config.get("ncr_curated_data"),
        entities=EntitiesConfig().entities,
        policy_tags=config.get("policy_tags", []),
        create_daily_tasks=False,
        dwh_import=dwh_import_read_replicas,
    )
    tasks = etl.render()
