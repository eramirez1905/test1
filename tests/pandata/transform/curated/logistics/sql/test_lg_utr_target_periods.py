import pytest


@pytest.mark.bigquery
def test_lg_utr_target_periods_sql(dry_run_query, read_logistics_sql):
    dry_run_query(read_logistics_sql("lg_utr_target_periods.sql"))
