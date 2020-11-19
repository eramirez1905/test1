import pytest


@pytest.mark.bigquery
def test_pd_accounting_vat_groups_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_accounting_vat_groups.sql"))
