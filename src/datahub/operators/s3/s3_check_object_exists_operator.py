from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.decorators import apply_defaults

# Constant used to push the value to xcom
S3_OBJECT_EXISTS = 'S3_OBJECT_EXISTS'


class S3CheckObjectExistsOperator(BaseOperator):
    """
    Check if an object exists on s3
    :param bucket_name: Source bucket name
    :type bucket_name: string
    :param object_name: Source object name
    :type object_name: string
    :param s3_conn_id: Connection id for s3
    :type s3_conn_id: string
    :param wild_card_match: Check for objects based on wildcard
    :type wild_card_match: boolean
    :param do_xcom_push: Push the result to xcom key S3_OBJECT_EXISTS
    :type do_xcom_push: boolean
    """
    template_fields = ('bucket_name', 'object_name', 'wildcard_match')

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 object_name: str,
                 s3_conn_id: str,
                 wildcard_match: bool,
                 do_xcom_push: bool,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.s3_conn_id = s3_conn_id
        self.wildcard_match = wildcard_match
        self.do_xcom_push = do_xcom_push

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        file_exists = s3_hook.check_for_wildcard_key(self.object_name,
                                                     self.bucket_name) if self.wildcard_match else s3_hook.check_for_key(
            self.object_name, self.bucket_name)
        if self.do_xcom_push:
            self.log.info(f"Pushing xcom key: {S3_OBJECT_EXISTS} with value: {file_exists}")
            context['task_instance'].xcom_push(key=S3_OBJECT_EXISTS, value=file_exists)
