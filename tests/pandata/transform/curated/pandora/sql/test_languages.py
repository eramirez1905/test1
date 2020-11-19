import pytest


@pytest.mark.bigquery
def test_languages_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("languages.sql"))
