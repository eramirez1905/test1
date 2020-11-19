import pytest


@pytest.mark.bigquery
def test_customer_addresses_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("customer_addresses.sql"))
