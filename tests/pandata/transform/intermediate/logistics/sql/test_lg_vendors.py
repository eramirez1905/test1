import pytest


@pytest.mark.bigquery
def test_lg_vendors_sql(dry_run_query, read_logistics_sql):
    dry_run_query(read_logistics_sql("lg_vendors.sql"))
