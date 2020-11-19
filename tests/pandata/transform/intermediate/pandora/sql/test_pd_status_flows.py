import pytest


@pytest.mark.bigquery
def test_pd_status_flows_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_status_flows.sql"))
