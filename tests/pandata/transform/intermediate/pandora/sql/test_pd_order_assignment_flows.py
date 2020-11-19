import pytest


@pytest.mark.bigquery
def test_pd_order_assignment_flows_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_order_assignment_flows.sql"))
