import logging
from typing import List

from load.s3.constants import S3_BUCKET_PANDORA
from utils.aws import list_child_dirs


def check_expected_child_dirs(
    aws_conn_id: str, parent_dir: str, expected_child_dirs: List[str]
):
    dirs = list_child_dirs(
        aws_conn_id=aws_conn_id, bucket_name=S3_BUCKET_PANDORA, prefix=f"{parent_dir}/"
    )
    differing_tables = set(dirs) - set(expected_child_dirs)
    logging.info(f"Differing_tables: {differing_tables}")
    assert len(differing_tables) == 0
