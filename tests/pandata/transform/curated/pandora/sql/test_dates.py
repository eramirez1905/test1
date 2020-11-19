import pytest


@pytest.mark.bigquery
def test_dates_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("dates.sql"))
