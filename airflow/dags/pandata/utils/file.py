from pathlib import Path

from configs import CONFIG


def read_file(filepath: Path) -> str:
    with open(filepath, "r") as f:
        return f.read()


def read_sql(filepath: Path, **additional_fields) -> str:
    query = read_file(filepath)
    return query.format(project_id=CONFIG.gcp.project, **additional_fields)
