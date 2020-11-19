import pytest


@pytest.mark.bigquery
def test_discounts_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("discounts.sql"))
