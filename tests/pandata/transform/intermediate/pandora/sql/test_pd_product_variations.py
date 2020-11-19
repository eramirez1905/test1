import pytest


@pytest.mark.bigquery
def test_pd_product_variations_sql(dry_run_query, read_pandora_sql):
    dry_run_query(read_pandora_sql("pd_product_variations.sql"))
