import pytest

from configs import CONFIG
from utils.file import read_file, read_sql


def test_read_file(tmp_path):
    fp = tmp_path / "test.txt"
    fp.write_text("foo")
    assert read_file(fp) == "foo"


@pytest.mark.parametrize(
    "query,expected",
    [
        ("SELECT 1 FROM {project_id}", f"SELECT 1 FROM {CONFIG.gcp.project}"),
        ("SELECT 1", "SELECT 1"),
    ],
)
def test_read_sql(tmp_path, query, expected):
    fp = tmp_path / "query.sql"
    fp.write_text(query)
    assert read_sql(fp) == expected


def test_read_sql_with_extra_args(tmp_path):
    fp = tmp_path / "query.sql"
    fp.write_text("SELECT 1 FROM {project_id} WHERE {foo}")
    expected = f"SELECT 1 FROM {CONFIG.gcp.project} WHERE bar"
    assert read_sql(fp, foo="bar") == expected
