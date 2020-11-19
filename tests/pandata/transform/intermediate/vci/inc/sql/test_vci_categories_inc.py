import pytest


@pytest.mark.bigquery
def test_vci_categories_inc_sql(dry_run_query, read_vci_sql):
    dry_run_query(read_vci_sql("vci_categories_inc.sql", created_date="2020-01-01"))
