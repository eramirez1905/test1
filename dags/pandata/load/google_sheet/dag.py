"""
Creates external google sheet tables if they don't exist.

To use this DAG, ensure that all data is on the first google sheet
and each google sheet can only be linked to one table.
"""
from airflow import DAG

from configs import CONFIG
from constants.airflow import DEFAULT_ARGS, GOOGLE_CLOUD_CONN
from configs.bigquery.datasets.constants import GOOGLE_SHEET_DATASET
from load.google_sheet.bigquery_google_sheet_operator import (
    BigQueryCreateGoogleSheetTableOperator,
)

with DAG(
    "load_google_sheet_import_tables_v1",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=CONFIG.airflow.dags.raw.schedule,
    tags=["load", "google_sheet"],
    doc_md=__doc__,
) as dag:

    DATASET = CONFIG.gcp.bigquery.get(GOOGLE_SHEET_DATASET)
    for t in DATASET.tables:
        BigQueryCreateGoogleSheetTableOperator(
            task_id=f"upsert_table__{t.name}",
            bigquery_conn_id=GOOGLE_CLOUD_CONN,
            destination_project_dataset_table=(
                f"{CONFIG.gcp.project}.{DATASET.name}.{t.name}"
            ),
            source_uri=t.source_uri,
            schema_fields=t.schema_fields_api_repr,
            skip_leading_rows=t.skip_leading_rows,
        )
