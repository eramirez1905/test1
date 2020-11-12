from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DeleteSyncedS3Objects(BaseOperator):
    """
    Operator to delete all objects from a bucket which were copied to the archive bucket
    """

    template_fields = ['object_keys']

    @apply_defaults
    def __init__(self, aws_conn_id: str, bucket_name: str, object_keys: str, *args, **kwargs):
        """
        :param aws_conn_id: Connection ID for S3
        :param bucket_name: Name of bucket to delete objects from
        :param object_keys: String-ified list of keys to delete. Ex: "['key1.csv', 'key1.csv']"
        """
        super(DeleteSyncedS3Objects, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.object_keys = object_keys

    def execute(self, context):
        s3_conn = S3Hook(aws_conn_id=self.aws_conn_id).get_conn()

        keys_as_list = eval(self.object_keys)
        if not keys_as_list:
            return

        objects = [{'Key': key} for key in keys_as_list]
        s3_conn.delete_objects(Bucket=self.bucket_name, Delete={'Objects': objects})
