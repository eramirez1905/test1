import pytest


@pytest.mark.bigquery
def test_countries_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("countries.sql"))
