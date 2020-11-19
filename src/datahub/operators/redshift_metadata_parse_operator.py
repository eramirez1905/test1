import json
import re
from tempfile import NamedTemporaryFile

from airflow import AirflowException
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedshiftMetadataParseOperator(BaseOperator):
    template_fields = ('aws_bucket', 'aws_prefix', 'gcs_bucket', 'gcs_prefix', 'delimiter')
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self,
                 aws_bucket,
                 aws_prefix,
                 gcs_bucket,
                 gcs_prefix,
                 delimiter='',
                 aws_conn_id='aws_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 verify=None,
                 *args,
                 **kwargs):
        super(RedshiftMetadataParseOperator, self).__init__(*args, **kwargs)
        self.aws_bucket = aws_bucket
        self.aws_prefix = aws_prefix
        self.gcs_bucket = gcs_bucket
        self.gcs_prefix = gcs_prefix
        self.delimiter = delimiter
        self.aws_conn_id = aws_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.verify = verify
        self._hook = None

    @property
    def aws_hook(self):
        if self._hook is None:
            self._hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        return self._hook

    def execute(self, context):
        self.log.info('Getting the metadata file from bucket: {0} in prefix: {1} (Delimiter {2})'.format(self.aws_bucket, self.aws_prefix, self.delimiter))

        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to,
        )

        with NamedTemporaryFile(delete=False, mode='w+') as temp_file:
            metadata = self.aws_hook.read_key(self.aws_prefix, self.aws_bucket)
            schema = self.parse_schema(metadata)
            json.dump(schema, temp_file)

        dest_filename = f'{self.gcs_prefix}/schema.json'
        self.log.info(f'Upload schema JSON file to {self.gcs_bucket}/{dest_filename}')
        gcs_hook.upload(
            bucket=self.gcs_bucket,
            object=dest_filename,
            mime_type='application/json',
            filename=temp_file.name,
        )

    @staticmethod
    def parse_schema(metadata_json):
        columns = json.loads(metadata_json)
        schema = []
        for column in columns.get('schema').get('elements'):
            dt = column.get('type', {}).get('base')
            if re.match(r'^character', dt):
                data_type = 'STRING'
            elif re.match(r'^numeric', dt):
                data_type = 'NUMERIC'
            elif re.match(r'^timestamp', dt):
                data_type = 'TIMESTAMP'
            elif re.match(r'^date$', dt):
                data_type = 'DATE'
            elif re.match(r'^bigint$|^integer$', dt):
                data_type = 'INT64'
            else:
                raise AirflowException(f'RedShift to BigQuery data type mapping not found for "{dt}".')

            schema.append({
                'type': data_type,
                'name': column.get('name'),
                'mode': 'NULLABLE'
            })

        return schema
