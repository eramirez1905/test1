import json
import tempfile
from datetime import datetime

import requests

from airflow import AirflowException
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from freshdesk.freshdesk_json_cleansing import FreshDeskJsonCleansing


class FreshdeskReportToGoogleCloudStorageOperator(BaseOperator):
    """
    Uploads a remote file to Google Cloud Storage.
    Optionally can compress the file for upload.

    :param dst: Destination path within the specified bucket. (templated)
    :type dst: string
    :param bucket: The bucket to upload to. (templated)
    :type bucket: string
    :param endpoint: Path to the remote file. (templated)
    :type endpoint: string
    :param request_params: Request parameters. (templated)
    :type request_params: dict
    :param google_cloud_storage_conn_id: The Airflow connection ID to upload with
    :type google_cloud_storage_conn_id: string
    :param mime_type: The mime-type string
    :type mime_type: string
    :param delegate_to: The account to impersonate, if any
    :type delegate_to: str
    :param gzip: Allows for file to be compressed and uploaded as gzip
    :type gzip: bool
    """
    template_fields = ('dst', 'bucket', 'request_params', 'endpoint')

    @apply_defaults
    def __init__(self,
                 region,
                 dst,
                 bucket,
                 endpoint,
                 http_conn_id,
                 request_params=None,
                 google_cloud_storage_conn_id='google_cloud_default',
                 mime_type='text/json',
                 delegate_to=None,
                 gzip=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.dst = dst
        self.bucket = bucket
        self.http_conn_id = http_conn_id
        self.request_params = request_params
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.gzip = gzip
        self.region = region

    def execute(self, context):
        http_hook = HttpHook(http_conn_id=self.http_conn_id, method='GET')
        self.log.info(f'Fetch report URL from {self.http_conn_id}')

        report_response = http_hook.run(self.endpoint, self.request_params)
        report = json.loads(report_response.text)

        if 'export' not in report:
            raise AirflowException(f'Freshdesk import failed to fetch the report url. Response={report_response.text}')

        report_url = report['export']['url']
        if report_url is None:
            raise AirflowException("Failed to get the report URL", report)

        self.log.info(f"Fetch report created_at {report['export']['created_at']} from {report_url}")

        with tempfile.NamedTemporaryFile(delete=True, mode='a+') as json_tmp_file:
            now_utc = datetime.utcnow()
            fresh_desk_json_cleansing = FreshDeskJsonCleansing(self.region,
                                                               now_utc.strftime('%Y-%m-%d %H:%M:%S.%f'))
            response = requests.get(report_url, stream=True)

            if response.status_code == 200:
                json_string = fresh_desk_json_cleansing.prepare_json(response)
                json_tmp_file.write(json_string)
            else:
                error_message = f"Error on fetching report URL from {report_url} with status code " \
                                f"{response.status_code}"
                self.log.error(error_message, response.text)
                raise AirflowException(error_message)

            hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to,
            )

            self.log.info(f"Upload CSV report to {self.bucket}/{self.dst}{json_tmp_file.name}")
            hook.upload(
                bucket=self.bucket,
                object=self.dst,
                mime_type=self.mime_type,
                filename=json_tmp_file.name,
                gzip=self.gzip,
            )
