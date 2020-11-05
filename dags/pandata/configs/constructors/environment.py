from dataclasses import dataclass
from typing import Optional

from configs.constructors.dataset import BigQueryConfig


@dataclass(frozen=True)
class Schedule:
    NONE = None
    HOURLY: str = "0 * * * *"
    DAILY: str = "0 0 * * *"
    WEEKLY: str = "0 0 * * 0"
    DAILY_GMT8_0630: str = "30 22 * * *"
    DAILY_GMT8_0650: str = "50 22 * * *"
    DAILY_GMT8_0725: str = "25 23 * * *"
    DAILY_GMT8_0815: str = "15 0 * * *"
    DAILY_GMT8_0900: str = "0 1 * * *"
    DAILY_GMT8_1000: str = "0 2 * * *"
    DAILY_GMT8_1410: str = "10 6 * * *"
    DAILY_GMT8_1455: str = "55 6 * * *"
    DAILY_GMT8_1545: str = "45 7 * * *"
    DAILY_GMT8_1800: str = "0 10 * * *"
    DAILY_GMT8_1810: str = "10 10 * * *"
    DAILY_GMT8_1900: str = "0 11 * * *"
    WEEKLY_GMT8_THU_1000 = "0 2 * * 4"


class BigQueryDatasetNotFound(BaseException):
    pass


@dataclass
class GCPConfig:
    project: str
    project_billing: str
    bigquery: BigQueryConfig
    bucket: str
    bucket_comp_intel_export: str
    bucket_mkt_cs_export: str
    bucket_ncr_export: str
    bucket_pandora_export: str
    bucket_pyspark: str
    bucket_pyspark_temp: str


@dataclass
class AWSConfig:
    bucket_vendor_points: str


@dataclass
class AirflowConnections:
    aws_dh_fridge: str
    task_failure_slack_alert: Optional[str] = None


@dataclass
class DAGConfig:
    schedule: Optional[str] = Schedule.NONE
    catchup: bool = False
    max_active_runs: int = 1


@dataclass
class DAGGroups:
    """
    Each config refers to one or more DAGs
    Create a new group if a new DAG that doesn't fall in any of these groups.
    """

    raw: DAGConfig = DAGConfig()
    curated: DAGConfig = DAGConfig()
    report: DAGConfig = DAGConfig()
    checks: DAGConfig = DAGConfig()
    raw__off_peak: DAGConfig = DAGConfig()
    dynamodb: DAGConfig = DAGConfig()
    incremental: DAGConfig = DAGConfig()
    promocodes: DAGConfig = DAGConfig()
    meta_search: DAGConfig = DAGConfig()

    start_of_day: DAGConfig = DAGConfig()
    hourly: DAGConfig = DAGConfig()
    daily: DAGConfig = DAGConfig()
    weekly: DAGConfig = DAGConfig()


@dataclass
class AirflowConfig:
    url: str
    dags: DAGGroups
    connections: AirflowConnections


@dataclass
class EnvironmentConfig:
    gcp: GCPConfig
    aws: AWSConfig
    airflow: AirflowConfig
