import pytest

from transform.curated.logistics.constants import SQL_DIR
from utils.file import read_sql


@pytest.fixture
def read_logistics_sql():
    def _read_sql(filepath):
        sql = read_sql(SQL_DIR / filepath)
        return sql

    return _read_sql
