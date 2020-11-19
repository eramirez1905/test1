import pytest


@pytest.mark.bigquery
def test_pd_fraud_validation_transactions_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_fraud_validation_transactions.sql"))
