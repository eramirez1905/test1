from monitor.staging_env.slack import create_logot_message, get_git_version

import pytest


GIT_VERSION = "v20.10.1"
COMMIT_HASH = "ace0585"
GIT_VERSION_TEXT = f"{GIT_VERSION} - {COMMIT_HASH}"


@pytest.fixture
def git_version_file(monkeypatch, tmp_path):
    monkeypatch.setenv("AIRFLOW_HOME", value=str(tmp_path))
    d = tmp_path / "airflow"
    d.mkdir()
    fp = d / "git_version"
    fp.write_text(GIT_VERSION_TEXT)
    yield
    fp.unlink()


def test_create_logot_message(git_version_file):
    assert (
        create_logot_message()
        == f"logot deploy airflow {GIT_VERSION} to staging eu pandata by <@UNQTP6S9Y>"
    )


def test_create_logot_message_empty():
    assert create_logot_message() is None


@pytest.mark.parametrize(
    "git_version,commit_hash", [("v1.0.0", "foobarr"), ("v20.10.121", "ACE0585")]
)
def test_get_git_version(git_version, commit_hash):
    assert get_git_version(f"{git_version} - {commit_hash}") == git_version
