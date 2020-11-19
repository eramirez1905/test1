import pytest


@pytest.mark.bigquery
def test_pd_calls_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_calls.sql"))
