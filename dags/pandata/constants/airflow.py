from datetime import datetime, timedelta

from utils.tasks import task_fail_slack_alert

AWS_CONN = "aws_pandata"
AWS_CONN_MKT_CS = "aws_mkt_cs_data"
GOOGLE_CLOUD_CONN = "bigquery_default"

DEFAULT_ARGS = {
    "owner": "APAC Data Team",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "concurrency": 20,
    "max_active_runs": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
    "executor_config": {
        "KubernetesExecutor": {
            "request_cpu": "200m",
            "limit_cpu": "200m",
            "request_memory": "500Mi",
            "limit_memory": "500Mi",
        },
    },
}
