import pytest


@pytest.mark.bigquery
def test_newsletter_users_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("newsletter_users.sql"))
