from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SyncS3BucketAcrossAccounts(BaseOperator):
    """
    Operator that copies all S3 objects in a bucket in one account to a bucket in any account (assuming ACL on the
    destination bucket allows this), setting the 'bucket-owner-full-control' ACL on the copied objects in the
    destination bucket.

    NOTE: When Airflow is bumped to 1.10.10, we could use the built-in S3CopyObjectOperator, but the target
    ACL cannot be set via this operator in 1.10.7.
    """

    @apply_defaults
    def __init__(self, aws_conn_id, source_bucket_name, dest_bucket_name, *args, dest_prefix=None, source_version_id=None, **kwargs):
        super(SyncS3BucketAcrossAccounts, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.destination_prefix = dest_prefix or ''
        self.source_bucket_name = source_bucket_name
        self.source_version_id = source_version_id
        self.dest_bucket_name = dest_bucket_name

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=False)
        s3_conn = s3_hook.get_conn()
        keys_to_sync = s3_hook.list_keys(self.source_bucket_name)

        if not keys_to_sync:
            return

        for source_key in keys_to_sync:
            copy_source = {
                'Bucket': self.source_bucket_name,
                'Key': source_key,
                'VersionId': self.source_version_id
            }
            destination_key = '/'.join([self.destination_prefix, source_key]).lstrip('/')
            s3_conn.copy_object(
                Bucket=self.dest_bucket_name,
                Key=destination_key,
                CopySource=copy_source,
                ACL='bucket-owner-full-control'
            )

        return keys_to_sync
