import pytest


@pytest.mark.bigquery
def test_pd_special_schedules_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_special_schedules.sql"))
