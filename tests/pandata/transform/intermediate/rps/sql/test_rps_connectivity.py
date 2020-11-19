import pytest


@pytest.mark.bigquery
def test_rps_connectivity_sql(dry_run_query, read_rps_sql):
    dry_run_query(read_rps_sql("rps_connectivity.sql"))
