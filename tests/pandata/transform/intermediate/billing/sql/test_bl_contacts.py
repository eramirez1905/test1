import pytest


@pytest.mark.bigquery
def test_bl_contacts_sql(dry_run_query, read_billing_sql):
    dry_run_query(read_billing_sql("bl_contacts.sql"))
