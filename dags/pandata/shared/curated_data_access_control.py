"""
Update shared curated data access control list tables in
`curated_data_filtered_pandata` and `curated_data_shared_pandata` dataset
"""
from airflow import DAG, conf

from datahub.curated_data.entities_config import EntitiesConfig
from datahub.curated_data.process_curated_data import ProcessCuratedData

from constants.airflow import DEFAULT_ARGS
from shared.configuration import shared_config
from configs import CONFIG

version = 1
dag_id_prefix = "shared_curated_data_access_control"

with DAG(
    dag_id=f"{dag_id_prefix}_v{version}",
    description=f"Update shared curated ACL views on BigQuery",
    schedule_interval=CONFIG.airflow.dags.report.schedule,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    template_searchpath=f"{conf.get('core', 'dags_folder')}/shared/curated_data/sql/",
    tags=["shared"],
    catchup=False,
    doc_md=__doc__,
) as dag:

    update_views = ProcessCuratedData(
        dag=dag,
        project_id=CONFIG.gcp.project,
        dataset_id=shared_config.get("bigquery").get("dataset").get("cl"),
        config=shared_config.get("curated_data"),
        entities=EntitiesConfig().entities,
        policy_tags=[],
        create_daily_tasks=True,  # only creates tasks to update views in filtered/shared datasets
        dwh_import=None,
    )

    tasks = update_views.render()
