import pytest


@pytest.mark.bigquery
def test_pd_dates_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_dates.sql"))
