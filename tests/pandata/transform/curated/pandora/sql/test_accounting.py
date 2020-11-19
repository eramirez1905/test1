import pytest


@pytest.mark.bigquery
def test_accounting_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("accounting.sql"))
