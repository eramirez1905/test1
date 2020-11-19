import pytest


@pytest.mark.bigquery
def test_lg_order_forecasts_sql(dry_run_query, read_logistics_sql):
    dry_run_query(read_logistics_sql("lg_order_forecasts.sql"))
