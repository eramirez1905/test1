import pytest


@pytest.mark.bigquery
def test_customers_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("customers.sql"))
