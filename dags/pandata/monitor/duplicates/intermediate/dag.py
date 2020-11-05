from airflow import DAG
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

from configs import CONFIG
from configs.bigquery.datasets.constants import INTERMEDIATE_DATASET
from constants.airflow import DEFAULT_ARGS, GOOGLE_CLOUD_CONN
from monitor.duplicates.constants import DUPLICATES_SQL
from utils.file import read_sql

with DAG(
    "check_duplicate_ids__intermediate_v1",
    default_args=DEFAULT_ARGS,
    schedule_interval=CONFIG.airflow.dags.checks.schedule,
    max_active_runs=1,
    catchup=False,
    tags=["monitor", "intermediate"],
    doc_md=__doc__,
) as dag:

    dataset = CONFIG.gcp.bigquery.get(INTERMEDIATE_DATASET)
    for t in dataset.tables:
        if t.fields:
            primary_keys = [f.name for f in t.fields if f.is_primary_key]
            if primary_keys:
                BigQueryCheckOperator(
                    task_id=f"check__{t.name}",
                    bigquery_conn_id=GOOGLE_CLOUD_CONN,
                    use_legacy_sql=False,
                    sql=read_sql(
                        DUPLICATES_SQL,
                        id_fields=",".join(primary_keys),
                        project=CONFIG.gcp.project,
                        dataset=dataset.name,
                        table=t.name,
                        partition_filter="",
                    ),
                )
