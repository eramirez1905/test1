import pytest


@pytest.mark.bigquery
def test_cp_addresses_sql(dry_run_query, read_corporate_sql):
    dry_run_query(read_corporate_sql("cp_addresses.sql"))
