from airflow import DAG
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

from configs import CONFIG
from configs.bigquery.datasets.constants import CURATED_DATASET
from constants.airflow import DEFAULT_ARGS, GOOGLE_CLOUD_CONN
from monitor.duplicates.constants import DUPLICATES_SQL
from utils.file import read_sql

with DAG(
    "check_duplicate_ids__curated_v1",
    default_args=DEFAULT_ARGS,
    schedule_interval=CONFIG.airflow.dags.checks.schedule,
    max_active_runs=1,
    catchup=False,
    tags=["monitor", "curated"],
    doc_md=__doc__,
) as dag:

    dataset = CONFIG.gcp.bigquery.get(CURATED_DATASET)
    for t in dataset.tables:
        BigQueryCheckOperator(
            task_id=f"check__{t.name}",
            bigquery_conn_id=GOOGLE_CLOUD_CONN,
            use_legacy_sql=False,
            sql=read_sql(
                DUPLICATES_SQL,
                id_fields=",".join([f.name for f in t.fields if f.is_primary_key]),
                project=CONFIG.gcp.project,
                dataset=dataset.name,
                table=t.name,
                partition_filter=(
                    f"WHERE {t.time_partitioning_config.field} = DATE('{{{{ ds }}}}')"
                    if t.time_partitioning_config
                    else ""
                ),
            ),
        )
