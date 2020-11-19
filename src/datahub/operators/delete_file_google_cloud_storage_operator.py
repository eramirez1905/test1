from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DeleteFileToGoogleCloudStorageOperator(BaseOperator):
    """
    Delete a file on Google Cloud Storage.

    :param filename: Destination path within the specified bucket. (templated)
    :type filename: string
    :param bucket: The bucket to upload to. (templated)
    :type bucket: string
    :param google_cloud_storage_conn_id: The Airflow connection ID to upload with
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any
    :type delegate_to: str
    """
    template_fields = ('bucket', 'filename')

    @apply_defaults
    def __init__(self,
                 bucket,
                 filename,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.filename = filename
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to,
        )

        hook.delete(self.bucket, self.filename)
