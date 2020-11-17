import time

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datahub.hooks.bigquery_hook import BigQueryHook


class BigQueryExportQueryToGCSOperator(BaseOperator):
    """
    Exports results of a BigQuery query to a Google Cloud Storage bucket.
    .. seealso::
        For more details about these parameters:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs
    :param sql: the sql code to be executed (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'.
    :param destination_cloud_storage_uris: The destination Google Cloud
        Storage URI (e.g. gs://some-bucket/some-file.txt). (templated) Follows
        convention defined here:
        https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
    :type destination_cloud_storage_uris: list
    :param compression: Type of compression to use.
    :type compression: str
    :param export_format: File format to export.
    :type export_format: str
    :param field_delimiter: The delimiter to use when extracting to a CSV.
    :type field_delimiter: str
    :param print_header: Whether to print a header for a CSV file extract.
    :type print_header: bool
    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: str
    :param temporary_dataset: dataset to write temporary output to
    :type temporary_dataset: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :type labels: dict
    :param priority: Specifies a priority for the query.
        Possible values include INTERACTIVE and BATCH.
        The default value is BATCH.
    :type priority: str
    """
    template_fields = ('sql', 'destination_cloud_storage_uris', 'labels')
    template_ext = ('.sql',)
    ui_color = '#e4e6f0'

    @apply_defaults
    def __init__(self,
                 sql,
                 destination_cloud_storage_uris,
                 query_params=None,
                 compression='GZIP',
                 export_format='CSV',
                 field_delimiter=',',
                 print_header=True,
                 bigquery_conn_id='bigquery_default',
                 temporary_dataset='temp',
                 delegate_to=None,
                 labels=None,
                 priority='BATCH',
                 *args,
                 **kwargs):
        super(BigQueryExportQueryToGCSOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.destination_cloud_storage_uris = destination_cloud_storage_uris
        self.query_params = query_params
        self.compression = compression
        self.export_format = export_format
        self.field_delimiter = field_delimiter
        self.print_header = print_header
        self.bigquery_conn_id = bigquery_conn_id
        self.temporary_dataset = temporary_dataset
        self.delegate_to = delegate_to
        self.labels = labels
        self.priority = priority

    def execute(self, context):
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                            delegate_to=self.delegate_to)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # use timestamp plus hash(destination_cloud_storage_uris) (to stay collision free)
        # as suffix for 'temp' table
        timestamp = str(time.time()).replace('.', '')

        destination_cloud_storage_uris_str = hash(tuple(self.destination_cloud_storage_uris))
        hash_8chr = str(hash(destination_cloud_storage_uris_str))[-8:]

        # TODO: could possibly take job_id/task_id as table name?!
        dataset_table = f'{self.temporary_dataset}.tmp_{timestamp}_{hash_8chr}'

        self.log.info('Running query and storing result in %s.', dataset_table)
        cursor.run_query(
            sql=self.sql,
            query_params=self.query_params,
            destination_dataset_table=dataset_table,
            write_disposition='WRITE_EMPTY',
            use_legacy_sql=False,
            priority=self.priority
        )

        self.log.info('Exporting result table to %s.', self.destination_cloud_storage_uris)
        cursor.run_extract(
            dataset_table,
            destination_cloud_storage_uris=self.destination_cloud_storage_uris,
            compression=self.compression,
            export_format=self.export_format,
            field_delimiter=self.field_delimiter,
            print_header=self.print_header,
            labels=self.labels,
        )

        self.log.info('Cleaning up: deleting %s.', dataset_table)
        cursor.run_table_delete(dataset_table)
