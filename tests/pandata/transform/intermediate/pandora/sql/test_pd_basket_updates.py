import pytest


@pytest.mark.bigquery
def test_pd_basket_updates_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_basket_updates.sql"))
