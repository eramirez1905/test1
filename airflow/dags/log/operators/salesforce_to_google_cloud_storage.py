import tempfile
import tenacity

from airflow import AirflowException
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.salesforce_hook import SalesforceHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from salesforce.salesforce_json_cleansing import SalesforceJsonCleansing


class SalesforceToGoogleCloudStorageOperator(BaseOperator):
    template_fields = ['gcs_object_name', 'execution_date', 'query']

    @apply_defaults
    def __init__(self,
                 instance,
                 gcs_bucket_name,
                 gcs_object_name,
                 salesforce_conn_id,
                 query,
                 execution_date,
                 google_cloud_storage_conn_id='google_cloud_default',
                 mime_type='text/json',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.instance = instance
        self.execution_date = execution_date
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_object_name = gcs_object_name
        self.salesforce_conn_id = salesforce_conn_id
        self.query = query
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.retry_args = dict(
            wait=tenacity.wait_exponential(multiplier=2),
            stop=tenacity.stop_after_attempt(6),
            retry=tenacity.retry_if_exception_type(Exception),
        )

    def execute(self, context):
        salesforce_hook = SalesforceHook(self.salesforce_conn_id)
        # Hook to upload the payload to google cloud storage
        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to,
        )
        records = ''
        try:
            query_results = salesforce_hook.make_query(self.query)
            if query_results.get('totalSize', 0) > 0:
                salesforce_json_cleaning = SalesforceJsonCleansing(self.execution_date)
                records = salesforce_json_cleaning.prepare(query_results)
            else:
                self.log.info(f'No data returned by query: {self.query}')
            with tempfile.NamedTemporaryFile(delete=True, mode='a+') as tmp_file:
                tmp_file.write(records)
                self.log.info(f'Upload to {self.gcs_bucket_name}')
                gcs_hook.upload(
                    bucket=self.gcs_bucket_name,
                    object=f'{self.gcs_object_name}',
                    filename=tmp_file.name,
                    mime_type=self.mime_type,
                )
                self.log.info(f'Upload successful: {self.gcs_bucket_name}/{self.gcs_object_name}')
        except Exception:
            raise AirflowException(f'Failed to fetch data from salesforce {self.instance}')
