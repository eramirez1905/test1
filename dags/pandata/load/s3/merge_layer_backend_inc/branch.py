from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


def branch_if_objects_exist(
    google_cloud_storage_conn_id: str,
    bucket: str,
    prefix: str,
    true_branch: str,
    false_branch: str,
    delimiter: str = None,
):
    hook = GoogleCloudStorageHook(
        google_cloud_storage_conn_id=google_cloud_storage_conn_id,
    )
    files = hook.list(bucket=bucket, prefix=prefix, delimiter=delimiter)
    return true_branch if files else false_branch
