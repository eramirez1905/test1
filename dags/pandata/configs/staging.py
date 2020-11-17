from configs.bigquery.datasets.staging import BQ_CONFIG
from configs.constructors.environment import (
    AirflowConfig,
    AirflowConnections,
    AWSConfig,
    DAGConfig,
    DAGGroups,
    EnvironmentConfig,
    GCPConfig,
    Schedule,
)

CONFIG = EnvironmentConfig(
    aws=AWSConfig(bucket_vendor_points="batch-ingestion-bucket-stg"),
    gcp=GCPConfig(
        project="fulfillment-dwh-staging",
        project_billing="dhh---analytics-apac-admin",
        bigquery=BQ_CONFIG,
        bucket="datahub-airflow-pandata-us-staging",
        bucket_comp_intel_export="datahub-exports-staging-us-comp-intel",
        bucket_mkt_cs_export="datahub-exports-staging-us-mkt-cs",
        bucket_ncr_export="datahub-exports-staging-us-ncr",
        bucket_pandora_export="datahub-exports-staging-us-pandora",
        bucket_pyspark="datahub-pyspark-us-staging",
        bucket_pyspark_temp="datahub-pyspark-temp-us-staging",
    ),
    airflow=AirflowConfig(
        url="https://airflow-st-pandata.fulfillment-dwh.com",
        dags=DAGGroups(
            raw=DAGConfig(schedule=Schedule.DAILY_GMT8_1410),
            curated=DAGConfig(schedule=Schedule.DAILY_GMT8_1455),
            report=DAGConfig(schedule=Schedule.DAILY_GMT8_1545),
            checks=DAGConfig(schedule=Schedule.DAILY_GMT8_1545),
            raw__off_peak=DAGConfig(schedule=Schedule.DAILY_GMT8_1810),
            dynamodb=DAGConfig(schedule=Schedule.DAILY_GMT8_1900),
            incremental=DAGConfig(
                schedule=Schedule.DAILY, max_active_runs=1, catchup=False
            ),
            meta_search=DAGConfig(schedule=Schedule.DAILY_GMT8_0900),
            start_of_day=DAGConfig(schedule=Schedule.DAILY_GMT8_1000),
            hourly=DAGConfig(schedule=Schedule.HOURLY),
            daily=DAGConfig(schedule=Schedule.DAILY),
            weekly=DAGConfig(schedule=Schedule.WEEKLY),
        ),
        connections=AirflowConnections(
            aws_dh_fridge="aws_pandata_dh_fridge_stg",
            task_failure_slack_alert="slack__apac_data_infra",
        ),
    ),
)
