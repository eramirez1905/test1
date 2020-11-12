import re
import os
from typing import Optional

from utils.file import read_file


def create_logot_message() -> Optional[str]:
    git_version_fp = os.path.join(
        os.getenv("AIRFLOW_HOME", default=""), "airflow", "git_version"
    )
    try:
        git_version = get_git_version(read_file(git_version_fp).strip())
        # slack user id of Aaron Mak
        return (
            f"logot deploy airflow {git_version} to staging eu pandata by <@UNQTP6S9Y>"
        )
    except FileNotFoundError:
        return None


def get_git_version(text: str) -> Optional[str]:
    matches = re.match(r"(^v\d+\.\d+\.\d+)\s-\s.*", text)
    if matches:
        return matches.group(1)
