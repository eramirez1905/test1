from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from apiclient.discovery import build


class GoogleSheetsHook(GoogleCloudBaseHook):
    """
    Interact with Google Sheets Storage. This hook uses the Google Cloud Platform
    connection.
    """

    def get_records(self, sql):
        raise NotImplementedError()

    def get_pandas_df(self, sql):
        raise NotImplementedError()

    def run(self, sql):
        raise NotImplementedError()

    def __init__(self,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None):
        super(GoogleSheetsHook, self).__init__(google_cloud_storage_conn_id, delegate_to)

    def get_conn(self):
        """
        Returns a Google Cloud Storage service object.
        """
        http_authorized = self._authorize()
        return build('sheets', 'v4', http=http_authorized, cache_discovery=False)

    def download(self, spreadsheet_id, spreadsheet_range):
        """
        Get a file from Google Cloud Storage.

        """

        service = self.get_conn()
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=spreadsheet_range).execute()

        return result
