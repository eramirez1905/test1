from configs import ENVIRONMENT

SPARK_BIGQUERY_CONNECTOR = "gs://spark-lib/bigquery/spark-bigquery-latest.jar"

DATAPROC_NAME = f"gcs-to-bq--ml-billing--{ENVIRONMENT}"

PREPROCESSED_TABLES = ["fee", "invoice"]
