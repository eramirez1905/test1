import pytest


@pytest.mark.bigquery
def test_users_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("users.sql"))
