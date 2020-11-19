import pytest


@pytest.mark.bigquery
def test_pd_payment_types_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_payment_types.sql"))
