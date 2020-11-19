from typing import Callable

import pytest
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from googleapiclient.discovery import Resource

from configs import CONFIG
from constants.airflow import GOOGLE_CLOUD_CONN


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "bigquery: requires an active Google Cloud connection "
        + "(deselect with '-m \"not bigquery\"')",
    )


@pytest.fixture(scope="session")
def google_cloud_client() -> Resource:
    service = BigQueryHook(bigquery_conn_id=GOOGLE_CLOUD_CONN).get_service()
    return service


@pytest.fixture(scope="session")
def dry_run_query(google_cloud_client) -> Callable[[str], None]:
    def _dry_run_query(sql):
        (
            google_cloud_client.jobs()
            .query(
                projectId=CONFIG.gcp.project_billing,
                body={"query": sql, "useLegacySql": False, "dryRun": True},
            )
            .execute()
        )

    return _dry_run_query
