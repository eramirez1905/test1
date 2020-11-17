import json
import os
from tempfile import mkdtemp

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class JSONToS3Operator(BaseOperator):

    template_fields = ('dict', 's3_key')

    @apply_defaults
    def __init__(self, dictionary, key, bucket, s3_conn_id=None, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self._hook = None
        self.s3_conn_id = s3_conn_id

        self.s3_key = key
        self.s3_bucket = bucket

        self.dict = dictionary

    def _build_hook(self):
        try:
            from airflow.hooks.S3_hook import S3Hook
            return S3Hook(self.s3_conn_id)
        except ImportError:
            self.log.error(
                'Could not create an S3Hook with connection id "%s". '
                'Please make sure that airflow[s3] is installed and '
                'the S3 connection exists.', self.s3_conn_id
            )

    @property
    def hook(self):
        if self._hook is None:
            self._hook = self._build_hook()
        return self._hook

    def execute(self, context):
        tmp_file = os.path.join(mkdtemp(), 'tmp_file.json')
        json.dump(self.dict, open(tmp_file, 'w'))

        self.hook.load_file(
            filename=tmp_file,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )

        os.remove(tmp_file)
