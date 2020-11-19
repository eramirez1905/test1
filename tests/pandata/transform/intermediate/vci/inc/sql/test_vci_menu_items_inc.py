import pytest


@pytest.mark.bigquery
def test_vci_menu_items_inc_sql(dry_run_query, read_vci_sql):
    dry_run_query(read_vci_sql("vci_menu_items_inc.sql", created_date="2020-01-01"))
