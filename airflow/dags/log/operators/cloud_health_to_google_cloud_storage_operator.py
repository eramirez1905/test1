import json
import tempfile

from airflow import AirflowException
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datahub.hooks.http_api_hook import HttpApiHook
from operators.cloud_health.cloud_health_json_cleansing import CloudHealthJsonCleansing


class CloudHealthOperatorToGoogleCloudStorageOperator(BaseOperator):
    template_fields = ['gcs_bucket_name', 'gcs_object_name', 'report', 'params']
    ui_color = '#57c8bd'
    ui_fgcolor = '#000'

    @apply_defaults
    def __init__(self,
                 report,
                 gcs_bucket_name,
                 gcs_object_name,
                 cloud_health_conn_id='cloud_health_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 is_backfill=False,
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.report = report
        self.gcs_object_name = gcs_object_name
        self.gcs_bucket_name = gcs_bucket_name
        self.cloud_health_conn_id = cloud_health_conn_id
        self.delegate_to = delegate_to
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id

        self.params = {
            "interval": self.report["interval"],
            "dimensions[]": self.report["dimensions"],
            "filters[]": self.__get_filters(is_backfill),
            "measures[]": self.report["measures"],
        }

    def __get_filters(self, is_backfill):
        # https://help.cloudhealthtech.com/faqs/#825cfab8-35a4-5ae3-89d9-472ebe13e76e
        # Once a month has ended, the cloud provider starts its closing process where all of that month’s cost information is finalized
        # and becomes the official bill, which is then generated and invoiced by the cloud provider.
        # During this closing and invoicing process, there is a delay of up to about 4 business days during which no bills
        # for the new month are delivered by the cloud provider.
        time_filter = [
            "time:select:{% for day in range(-4,0) %}{{ macros.ds_add(ds, day) }},{% endfor %}{{ds}}"
        ]
        if is_backfill:
            time_filter = []
        time_filter = self.report["filters"] + time_filter
        return time_filter

    def execute(self, context):
        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to,
        )

        json_cleaning = CloudHealthJsonCleansing(self.report["type"], context["execution_date"])
        try:
            records = json_cleaning.prepare(self.get_json_response())
            with tempfile.NamedTemporaryFile(delete=False, mode='a+') as tmp_file:
                tmp_file.write(records)
                self.log.info(f'Upload {tmp_file.name} to {self.gcs_bucket_name}')
                gcs_hook.upload(
                    bucket=self.gcs_bucket_name,
                    object=f'{self.gcs_object_name}',
                    filename=tmp_file.name,
                    mime_type='text/json',
                )
                self.log.info(f'Upload successful: {self.gcs_bucket_name}/{self.gcs_object_name}')
        except Exception:
            raise AirflowException(f'Failed to fetch data from CloudHealth')

    def get_json_response(self) -> dict:
        api_hook = HttpApiHook(http_conn_id=self.cloud_health_conn_id, method='GET')
        self.log.info("params: %s", self.params)
        devices_response = api_hook.run(
            endpoint=f"olap_reports/{self.report['type']}/{self.report['name']}",
            data=self.params
        )
        self.log.debug('CloudHealth response: %s', devices_response.text)

        return json.loads(devices_response.text, parse_int=str, parse_float=str)
