import os

from airflow.hooks.S3_hook import S3Hook


def list_child_dirs(aws_conn_id: str, bucket_name: str, prefix: str):
    hook = S3Hook(aws_conn_id=aws_conn_id)
    fullpaths = hook.list_prefixes(
        bucket_name=bucket_name, prefix=prefix, delimiter="/"
    )
    return [os.path.basename(os.path.dirname(fp)) for fp in fullpaths]
