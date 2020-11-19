import pytest


@pytest.mark.bigquery
def test_vci_categories_sql(dry_run_query, read_vci_sql):
    dry_run_query(read_vci_sql("vci_categories.sql"))
