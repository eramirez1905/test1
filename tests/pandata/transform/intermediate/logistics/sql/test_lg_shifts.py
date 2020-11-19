import pytest


@pytest.mark.bigquery
def test_lg_shifts_sql(dry_run_query, read_logistics_sql):
    dry_run_query(read_logistics_sql("lg_shifts.sql"))
