import pytest


@pytest.mark.bigquery
def test_pd_menus_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_menus.sql"))
