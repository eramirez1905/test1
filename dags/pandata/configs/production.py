from configs.bigquery.datasets.production import BQ_CONFIG
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
    aws=AWSConfig(bucket_vendor_points="batch-ingestion-bucket-prod"),
    gcp=GCPConfig(
        project="fulfillment-dwh-production",
        project_billing="dhh---analytics-apac-admin",
        bigquery=BQ_CONFIG,
        bucket="datahub-airflow-pandata-us-production",
        bucket_comp_intel_export="datahub-exports-production-us-comp-intel",
        bucket_mkt_cs_export="datahub-exports-production-us-mkt-cs",
        bucket_ncr_export="datahub-exports-production-us-ncr",
        bucket_pandora_export="datahub-exports-production-us-pandora",
        bucket_pyspark="datahub-pyspark-us-production",
        bucket_pyspark_temp="datahub-pyspark-temp-us-production",
    ),
    airflow=AirflowConfig(
        url="https://airflow-pandata.fulfillment-dwh.com",
        dags=DAGGroups(
            raw=DAGConfig(schedule=Schedule.DAILY_GMT8_0650),
            curated=DAGConfig(schedule=Schedule.DAILY_GMT8_0725),
            report=DAGConfig(schedule=Schedule.DAILY_GMT8_0815),
            checks=DAGConfig(schedule=Schedule.DAILY_GMT8_0815),
            raw__off_peak=DAGConfig(schedule=Schedule.DAILY_GMT8_1800),
            dynamodb=DAGConfig(schedule=Schedule.DAILY_GMT8_1900),
            incremental=DAGConfig(
                schedule=Schedule.DAILY, max_active_runs=5, catchup=True
            ),
            meta_search=DAGConfig(schedule=Schedule.DAILY_GMT8_0900),
            start_of_day=DAGConfig(schedule=Schedule.DAILY_GMT8_1000),
            hourly=DAGConfig(schedule=Schedule.HOURLY),
            daily=DAGConfig(schedule=Schedule.DAILY),
            weekly=DAGConfig(schedule=Schedule.WEEKLY),
            promocodes=DAGConfig(schedule=Schedule.WEEKLY_GMT8_THU_1000),
        ),
        connections=AirflowConnections(
            aws_dh_fridge="aws_pandata_dh_fridge_prod",
            task_failure_slack_alert="slack__just_data_apac",
        ),
    ),
)
