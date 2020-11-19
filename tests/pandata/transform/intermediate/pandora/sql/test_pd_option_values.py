import pytest


@pytest.mark.bigquery
def test_pd_option_values_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_option_values.sql"))
