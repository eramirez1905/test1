import pytest


@pytest.mark.bigquery
def test_lg_rider_wallet_transactions_sql(dry_run_query, read_logistics_sql):
    dry_run_query(read_logistics_sql("lg_rider_wallet_transactions.sql"))
