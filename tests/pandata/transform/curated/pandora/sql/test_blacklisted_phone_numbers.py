import pytest


@pytest.mark.bigquery
def test_blacklisted_phone_numbers_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("blacklisted_phone_numbers.sql"))
