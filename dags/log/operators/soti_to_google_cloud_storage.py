import json
import tempfile
import time
from airflow.models import BaseOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from datahub.hooks.http_hook import HttpHookWithoutAuthentication
from airflow.hooks.http_hook import HttpHook as HttpHookBase
from airflow import AirflowException
from airflow.utils.decorators import apply_defaults
from soti.soti_json_cleansing import SotiJsonCleansing


class SotiToGoogleCloudStorage(BaseOperator):

    template_fields = ['gcs_object_name', 'execution_date']

    @apply_defaults
    def __init__(self,
                 region,
                 gcs_bucket_name,
                 gcs_object_name,
                 execution_date,
                 pagination_limit=50,
                 pagination_size=10000,
                 soti_conn_id='http_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 mime_type='text/json',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.region = region
        self.execution_date = execution_date
        self.pagination_limit = pagination_limit
        self.pagination_size = pagination_size
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_object_name = gcs_object_name
        self.soti_conn_id = soti_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to

    def execute(self, context):
        # Hook to get the access token
        oauth_hook = HttpHookBase(http_conn_id=f'{self.soti_conn_id}_oauth', method='POST')

        api_connection_credentials = self.check_connection_parameters(oauth_hook=oauth_hook, conn="api")
        self.check_connection_parameters(oauth_hook=oauth_hook, conn="oauth")

        token_response = oauth_hook.run(endpoint='MobiControl/api/token',
                                        headers={
                                            'Content-Type': 'application/x-www-form-urlencoded',
                                        },
                                        data={
                                            'grant_type': 'password',
                                            'username': api_connection_credentials.login,
                                            'password': api_connection_credentials.password,
                                        },)
        json_token_response = json.loads(token_response.text)

        if 'access_token' not in json_token_response:
            raise AirflowException(f'SOTI import failed to get the access token. Response={token_response.text}')

        access_token = json_token_response['access_token']
        headers = {'Authorization': f'Bearer {access_token}'}
        self.log.info(f'Successfully got SOTI token for {self.soti_conn_id}')

        # Hook to get the data from SOTI devices endpoint.
        counter = 0
        to_skip = 0

        with tempfile.NamedTemporaryFile(delete=True, mode='a+') as tmp_file:
            devices_json = self.get_json_response(headers=headers, pagination_size=self.pagination_size,
                                                  to_skip=to_skip)
            soti_json_cleansing = SotiJsonCleansing(self.region, self.execution_date)

            while devices_json:
                if counter >= self.pagination_limit:
                    raise AirflowException(f'SOTI Import to GCS pagination limit reached: {counter} iterations.')

                devices_str_json = soti_json_cleansing.prepare(devices_json)
                tmp_file.write(devices_str_json + '\n')
                # For next iteration:
                counter += 1
                to_skip = counter * self.pagination_size
                devices_json = self.get_json_response(headers=headers, pagination_size=self.pagination_size,
                                                      to_skip=to_skip)
                time.sleep(1)
                self.log.info(f'Successfully got SOTI data for {self.soti_conn_id} , counter {counter}, '
                              f'taking {self.pagination_size}')

            # Hook to upload the payload to google cloud storage
            gcs_hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to
            )

            self.log.info(f"Upload to {self.gcs_bucket_name}/{self.gcs_object_name}")
            gcs_hook.upload(
                bucket=self.gcs_bucket_name,
                object=self.gcs_object_name,
                filename=tmp_file.name,
                mime_type=self.mime_type
            )

    def get_json_response(self, headers, pagination_size, to_skip):
        api_hook = HttpHookWithoutAuthentication(http_conn_id=f'{self.soti_conn_id}_api', method='GET')
        devices_response = api_hook.run(endpoint=f'MobiControl/api/devices?order=DeviceId,MACAddress&skip={to_skip}&'
                                                 f'take={pagination_size}',
                                        headers=headers,
                                        )
        return json.loads(devices_response.text, parse_int=str, parse_float=str)

    def check_connection_parameters(self, oauth_hook, conn):
        if conn == 'api':
            client_id = 'login'
            client_secret = 'password'
        else:
            client_id = 'client_id'
            client_secret = 'client_secret'
        try:
            connection_credentials = oauth_hook.get_connection(f'{self.soti_conn_id}_{conn}')
            if not connection_credentials.host:
                raise AirflowException('No host defined')
            if not connection_credentials.login:
                raise AirflowException(f'No {client_id} defined')
            if not connection_credentials.password:
                raise AirflowException(f'No {client_secret} defined')
            return connection_credentials
        except AirflowException as air_err:
            raise AirflowException(f'The SOTI connection {self.soti_conn_id}_{conn} does not exists.'
                                   f'Please create it and store host, {client_id}, and {client_secret}.', air_err)
