import pytest


@pytest.mark.bigquery
def test_fraud_validation_transactions_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("fraud_validation_transactions.sql"))
