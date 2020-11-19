import json
import tempfile

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from godroid.godroid_json_cleansing import GodroidJsonCleansing


class GodroidToGoogleCloudStorage(BaseOperator):
    template_fields = ['gcs_object_name', 'execution_date']

    @apply_defaults
    def __init__(self,
                 region,
                 gcs_bucket_name,
                 gcs_object_name,
                 endpoint,
                 execution_date,
                 google_cloud_conn_id='google_cloud_default',
                 mime_type='text/json',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.region = region
        self.execution_date = execution_date
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_object_name = gcs_object_name
        self.endpoint = endpoint
        self.google_cloud_conn_id = google_cloud_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to

    def execute(self, context):
        authenticated_session = self.get_authenticated_session(gcp_conn_id=self.google_cloud_conn_id)
        devices_json = self.get_json_response(authenticated_session=authenticated_session, endpoint=self.endpoint)
        godroid_json_cleansing = GodroidJsonCleansing(region=self.region, execution_date=self.execution_date)
        with tempfile.NamedTemporaryFile(delete=True, mode='a+') as tmp_file:
            if devices_json is not None:
                devices_str_json = godroid_json_cleansing.prepare(devices_json)
                tmp_file.write(devices_str_json + '\n')
            # Hook to upload the payload to google cloud storage
            gcs_hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.google_cloud_conn_id,
                delegate_to=self.delegate_to
            )
            self.log.info(f'Upload to {self.gcs_bucket_name}')
            gcs_hook.upload(
                bucket=self.gcs_bucket_name,
                object=f'{self.gcs_object_name}',
                filename=tmp_file.name,
                mime_type=self.mime_type
            )
            self.log.info(f'Upload successful: {self.gcs_bucket_name}/{self.gcs_object_name}')

    def get_json_response(self, authenticated_session, endpoint):
        self.log.info(f'Getting firebase devices for: {endpoint}')
        try:
            response = authenticated_session.request(endpoint)
            if response[0].status == 200:
                response_content = response[1]
                self.log.info(f'Successfully got firebase devices for: {endpoint}')
                return json.loads(response_content)
            else:
                raise AirflowException(
                    f'Failed to fetch data from firebase devices for {endpoint} response code: {response[0].status}')
        except Exception:
            raise AirflowException(f'Failed to fetch data from firebase devices for {endpoint}')

    def get_authenticated_session(self, gcp_conn_id):
        gcp_hook = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id, delegate_to=self.delegate_to)
        try:
            return gcp_hook._authorize()
        except Exception:
            raise AirflowException(f'Failed to get authorized session for connection id {gcp_conn_id}, please check credentials and scopes')
