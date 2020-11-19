from datetime import datetime

from utils.tasks import create_fail_slack_message


def test_create_fail_slack_message(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")

    class TaskInstanceStub:
        dag_id = "dag_id"
        task_id = "task_id"
        log_url = "http://127.0.0.1:8080/airflow/admin/airflow/logs"

    context = {
        "task_instance": TaskInstanceStub(),
        "execution_date": datetime.strptime(
            "2019-11-11T03:34:25.971046", "%Y-%m-%dT%H:%M:%S.%f"
        ),
        "next_execution_date": datetime.strptime(
            "2019-11-12T03:34:25.971046", "%Y-%m-%dT%H:%M:%S.%f"
        ),
    }
    timestamp = round(context["next_execution_date"].timestamp())
    expected = (
        f":scream: *task_id <!date^{timestamp}^failed "
        "{date_pretty} at {time}|2019-11-12 03:34:25.971046>*\n"
        "*Environment*: Datahub - dev\n"
        "*DAG*: dag_id\n"
        "<http://127.0.0.1:8080/airflow/admin/airflow/logs|*View Logs*>"
    )

    assert create_fail_slack_message(context) == expected
