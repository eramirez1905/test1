import pytest


@pytest.mark.bigquery
def test_vendors_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("vendors.sql"))
