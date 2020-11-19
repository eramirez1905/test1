import pytest


@pytest.mark.bigquery
def test_sb_subscription_payments_sql(dry_run_query, read_subscription_sql):
    dry_run_query(read_subscription_sql("sb_subscription_payments.sql"))
