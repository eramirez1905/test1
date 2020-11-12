import re

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

from configs import CONFIG, ENVIRONMENT


def create_fail_slack_message(context: dict) -> str:
    task_instance = context["task_instance"]
    next_exec_date = context.get("next_execution_date")
    exec_date = context["execution_date"] if next_exec_date is None else next_exec_date
    timestamp = round(exec_date.timestamp())
    return (
        f":scream: *{task_instance.task_id} "
        f"<!date^{timestamp}^failed {{date_pretty}} at {{time}}|{exec_date}>*\n"
        f"*Environment*: Datahub - {ENVIRONMENT}\n"
        f"*DAG*: {task_instance.dag_id}\n"
        f"<{task_instance.log_url}|*View Logs*>"
    )


def task_fail_slack_alert(context: dict):
    failed_alert = SlackWebhookOperator(
        task_id="slack_fail_alert",
        http_conn_id=CONFIG.airflow.connections.task_failure_slack_alert,
        message=create_fail_slack_message(context=context),
    )
    return failed_alert.execute(context=context)


def replace_non_alnum_chars(table_name: str) -> str:
    return re.sub(r"\W+", "_", table_name)
