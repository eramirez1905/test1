import pytest


@pytest.mark.bigquery
def test_lg_rider_deposit_payments_sql(dry_run_query, read_logistics_sql):
    dry_run_query(read_logistics_sql("lg_rider_deposit_payments.sql"))
