import pytest


@pytest.mark.bigquery
def test_lg_payments_quest_rules_sql(dry_run_query, read_logistics_sql):
    dry_run_query(read_logistics_sql("lg_payments_quest_rules.sql"))
