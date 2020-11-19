import pytest


@pytest.mark.bigquery
def test_basket_updates_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("basket_updates.sql"))
