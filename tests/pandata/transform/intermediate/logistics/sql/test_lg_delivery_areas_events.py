import pytest


@pytest.mark.bigquery
def test_lg_delivery_areas_events_sql(dry_run_query, read_logistics_sql):
    dry_run_query(read_logistics_sql("lg_delivery_areas_events.sql"))
