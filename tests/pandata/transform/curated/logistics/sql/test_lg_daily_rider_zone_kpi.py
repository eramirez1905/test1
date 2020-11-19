import pytest


@pytest.mark.bigquery
def test_lg_daily_rider_zone_kpi_sql(dry_run_query, read_logistics_sql):
    dry_run_query(read_logistics_sql("lg_daily_rider_zone_kpi.sql"))
