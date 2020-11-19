import pytest

from transform.intermediate.vci.inc.constants import SQL_DIR
from utils.file import read_sql


@pytest.fixture
def read_vci_sql():
    def _read_sql(filepath, **kwargs):
        sql = read_sql(SQL_DIR / filepath, **kwargs)
        return sql

    return _read_sql
