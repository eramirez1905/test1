import pytest


@pytest.mark.bigquery
def test_pd_menu_categories_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_menu_categories.sql"))
