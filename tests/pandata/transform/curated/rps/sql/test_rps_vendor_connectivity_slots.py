import pytest


@pytest.mark.bigquery
def test_rps_vendor_connectivity_slots_sql(dry_run_query, read_rps_sql):
    dry_run_query(read_rps_sql("rps_vendor_connectivity_slots.sql"))
