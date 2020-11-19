from pathlib import Path
from configs import ENVIRONMENT

CURRENT_DIR = Path(__file__).resolve().parent
SQL_DIR = CURRENT_DIR / "sql"
UPDATE_TABLE_SQL = SQL_DIR / "update_table.sql"

SPARK_BIGQUERY_CONNECTOR = "gs://spark-lib/bigquery/spark-bigquery-latest.jar"

DATAPROC_NAME = f"gcs-to-bq--ml-backend-inc--{ENVIRONMENT}"
