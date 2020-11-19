import pytest


@pytest.mark.bigquery
def test_pd_chain_menu_groups_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_chain_menu_groups.sql"))
