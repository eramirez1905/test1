"""
Sends a slack message to trigger deployment of staging environment
since staging is not deployed automatically but we need it to
be up to date with production
"""

from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

from configs import CONFIG
from constants.airflow import DEFAULT_ARGS
from monitor.staging_env.slack import create_logot_message


with DAG(
    "update_staging_env",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=CONFIG.airflow.dags.raw.schedule,
    tags=["monitor", "production_only"],
    doc_md=__doc__,
) as dag:
    SlackWebhookOperator(
        task_id="update_staging_with_logot",
        http_conn_id="slack__apac_data_infra",
        message=create_logot_message(),
    ),
