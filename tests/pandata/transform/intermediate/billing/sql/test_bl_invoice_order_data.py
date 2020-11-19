import pytest


@pytest.mark.bigquery
def test_bl_invoice_order_data_sql(dry_run_query, read_billing_sql):
    dry_run_query(read_billing_sql("bl_invoice_order_data.sql"))
