import pytest


@pytest.mark.bigquery
def test_vouchers_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("vouchers.sql"))
