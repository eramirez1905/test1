from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datahub.hooks.bigquery_hook import BigQueryHook


class BigQueryCopyTableSchemaOperator(BaseOperator):
    template_fields = ('source_dataset_id', 'source_table_id', 'destination_dataset_id', 'destination_table_id',
                       'destination_expiration_ms', 'labels')
    ui_color = '#7ca0e6'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(self,
                 source_dataset_id,
                 source_table_id,
                 destination_dataset_id,
                 destination_table_id,
                 destination_expiration_ms=None,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 labels=None,
                 *args, **kwargs):

        super(BigQueryCopyTableSchemaOperator, self).__init__(*args, **kwargs)

        self.source_dataset_id = source_dataset_id
        self.source_table_id = source_table_id
        self.destination_dataset_id = destination_dataset_id
        self.destination_table_id = destination_table_id
        self.destination_expiration_ms = destination_expiration_ms
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.labels = labels

    def execute(self, context):
        hook = BigQueryHook(
            bigquery_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to
        )

        if not hook.table_exists(hook.project_id, self.destination_dataset_id, self.destination_table_id):
            schema = hook.get_table_schema(self.source_dataset_id, self.source_table_id)

            schema_fields = schema['schema']['fields']
            time_partitioning = schema.get('timePartitioning', None)
            if time_partitioning and self.destination_expiration_ms:
                time_partitioning['expirationMs'] = self.destination_expiration_ms
            clustering = schema.get('clustering', None)

            self.log.info(f"Creating table {self.destination_dataset_id}_{self.destination_table_id}"
                          f" with expiration_ms={self.destination_expiration_ms}")
            hook.create_empty_table(dataset_id=self.destination_dataset_id,
                                    table_id=self.destination_table_id,
                                    schema_fields=schema_fields,
                                    time_partitioning=time_partitioning,
                                    clustering=clustering)
        else:
            self.log.info(f"Table {self.destination_dataset_id}_{self.destination_table_id} exists, nothing to do")
