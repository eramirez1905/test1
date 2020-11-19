import pytest


@pytest.mark.bigquery
def test_pd_vendors_cuisines_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_vendors_cuisines.sql"))
