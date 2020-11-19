import pytest

from transform.curated.subscription.constants import SQL_DIR
from utils.file import read_sql


@pytest.fixture
def read_subscription_sql():
    def _read_sql(filepath):
        sql = read_sql(SQL_DIR / filepath)
        return sql

    return _read_sql
