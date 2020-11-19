import pytest


@pytest.mark.bigquery
def test_sb_subscriptions_sql(dry_run_query, read_subscription_sql):
    dry_run_query(read_subscription_sql("sb_subscriptions.sql"))
