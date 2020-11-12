from configs.bigquery.datasets.staging import BQ_CONFIG
from configs.constructors.environment import (
    AirflowConfig,
    AirflowConnections,
    AWSConfig,
    DAGGroups,
    EnvironmentConfig,
    GCPConfig,
)

CONFIG = EnvironmentConfig(
    aws=AWSConfig(bucket_vendor_points="batch-ingestion-bucket-stg"),
    gcp=GCPConfig(
        project="fulfillment-dwh-staging",
        project_billing="dhh---analytics-apac-admin",
        bigquery=BQ_CONFIG,
        bucket="datahub-airflow-pandata-us-dev",
        bucket_comp_intel_export="datahub-exports-staging-us-comp-intel",
        bucket_mkt_cs_export="datahub-exports-staging-us-mkt-cs",
        bucket_ncr_export="datahub-exports-staging-us-ncr",
        bucket_pandora_export="datahub-exports-staging-us-pandora",
        bucket_pyspark="datahub-pandata-pyspark-dev",
        bucket_pyspark_temp="datahub-pandata-pyspark-temp-dev",
    ),
    airflow=AirflowConfig(
        url="http://127.0.0.1:8080",
        dags=DAGGroups(),
        connections=AirflowConnections(aws_dh_fridge="aws_pandata_dh_fridge_stg"),
    ),
)
