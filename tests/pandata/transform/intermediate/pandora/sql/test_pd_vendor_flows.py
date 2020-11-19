import pytest


@pytest.mark.bigquery
def test_pd_vendor_flows_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_vendor_flows.sql"))
