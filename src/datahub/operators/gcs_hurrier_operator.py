import os
import json
import pandas as pd

from typing import Dict, Callable, Union

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from datahub.hooks.usehurrier_hook import UseHurrierHook


class GoogleCloudStorageHurrierOperator(BaseOperator):
    """
    Load a file from Google Cloud Storage, apply some transformation and upload it to hurrier endpoint.

    :param bucket_name: Source bucket name
    :type bucket_name: string
    :param object_name: Source object name
    :type object_name: string
    :param endpoint: usehurrier endpoint
    :type endpoint: string
    :param data_parser: Function to read data with from file, returning a pd.DataFrame or Dict
    :type data_parser: Callable[[str], Union[pd.DataFrame, Dict]]
    :param data_transformer: Function to transform data (pd.DataFrame or Dict) with, returning a Dict that can
    be dumped to json
    :type data_transformer: Callable[[Union[pd.DataFrame, Dict]], Dict]
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param http_method: The HTTP method to use, default = "POST"
    :type http_method: string
    :param http_headers: The HTTP headers to be added to the request
    :type http_headers: a dictionary of string key/value pairs
    """
    template_fields = ('bucket_name', 'object_name', 'endpoint')
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 object_name: str,
                 country_code: str,
                 endpoint: str,
                 data_parser: Callable[[str], Union[pd.DataFrame, Dict]] = lambda x: pd.read_csv(x, sep=';'),
                 data_transformer: Callable[[Union[pd.DataFrame, Dict]], Dict] = lambda x: x.to_dict(items='records'),
                 gcp_conn_id: str = 'google_cloud_default',
                 http_conn_id: str = 'usehurrier',
                 http_method: str = 'POST',
                 http_headers: Dict[str, str] = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.country_code = country_code
        self.endpoint = endpoint
        self.data_parser = data_parser
        self.data_transformer = data_transformer
        self.gcp_conn_id = gcp_conn_id
        self.http_conn_id = http_conn_id
        self.http_method = http_method
        self.http_headers = http_headers

    def execute(self, context):
        gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcp_conn_id)

        # TODO: these should be self.log.info, but those are currently not shown
        self.log.warn("Reading from GCS: %s", self.object_name)
        filename = os.path.basename(self.object_name)
        gcs_hook.download(self.bucket_name, self.object_name, filename)

        self.log.warn("Parsing from file: %s", filename)
        data = self.data_parser(filename)
        os.remove(filename)

        if len(data) == 0:
            self.log.warn("Data is empty - nothing to do.")
            return

        # TODO: if the endpoint does not allow bulk uploads, we could iterate over records here
        self.log.warn("Transforming data.")
        data = self.data_transformer(data)

        self.log.warn("Preparing payload.")
        payload = json.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))
        headers = {'Content-Type': 'application/json'}

        self.log.warn("Calling HTTP method on %s with payload: \n %s", self.endpoint, payload)
        http_hook = UseHurrierHook(country_code=self.country_code,
                                   method=self.http_method,
                                   http_conn_id=self.http_conn_id)
        response = http_hook.run(endpoint=self.endpoint,
                                 data=payload,
                                 headers=headers)
        self.log.warn("Response status: %s", response.status_code)
        self.log.warn("Response text: %s", response.text)

        return response.text
