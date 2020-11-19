import pytest


@pytest.mark.bigquery
def test_cp_company_addresses_sql(dry_run_query, read_corporate_sql):
    dry_run_query(read_corporate_sql("cp_company_addresses.sql"))
