import datetime
from typing import Union

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import pendulum


class S3CheckLastObjectUpdateSensor(BaseSensorOperator):
    """
    Operator checks S3 object metadata to validate if it was modified since provided datetime.

    :param s3_key: The S3 key to use as the object name when uploading to AWS S3 including
        prefixes.
    :param s3_bucket: The S3 bucket to upload to.
    :param check_datetime: Datetime-like object or datetime string to check s3 object
        LastModified against.
    :param aws_conn_id: S3 connection ID.
    """

    template_fields = ("s3_key", "s3_bucket", "check_datetime")

    @apply_defaults
    def __init__(
        self,
        s3_key: str,
        s3_bucket: str,
        check_datetime: Union[str, datetime.datetime, pendulum.Pendulum],
        aws_conn_id: str = "aws_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.check_datetime = check_datetime
        self.aws_conn_id = aws_conn_id

    def poke(self, context):
        if isinstance(self.check_datetime, str):
            check_datetime = pendulum.parse(self.check_datetime)

        self.log.info(
            f"Checking if object s3://{self.s3_bucket}/{self.s3_key} was modifed since {check_datetime}"
        )
        hook = S3Hook(aws_conn_id=self.aws_conn_id)
        bucket = hook.get_bucket(self.s3_bucket)

        s3_object = bucket.Object(self.s3_key)

        if check_datetime > s3_object.get().get("LastModified"):
            return False
        return True
