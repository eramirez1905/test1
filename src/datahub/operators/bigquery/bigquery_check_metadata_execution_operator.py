import os

from airflow.utils.decorators import apply_defaults

from datahub.curated_data.metadata_execution_table import MetadataExecutionTable
from datahub.operators.bigquery.bigquery_check_operator import BigQueryCheckOperator


class BigQueryCheckMetadataExecutionOperator(BigQueryCheckOperator):
    ui_color = '#0c9e3d'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(self,
                 config,
                 dataset,
                 table_name,
                 metadata_execution_table: MetadataExecutionTable,
                 *args,
                 **kwargs):
        """
        The project_id is set to production for the check because raw tables in staging are
        not updated (ergo, their last execution date is in the past or non existent).
        Therefore, when testing it can lead to false failures.
        """

        super().__init__(
            sql='',
            use_legacy_sql=False,
            *args,
            **kwargs
        )

        self.project_id = config.get('bigquery_check_metadata_execution').get('project_id')
        self.dataset = dataset
        self.table_name = table_name
        self.params = {
            'project_id': self.project_id,
            'metadata_dataset': metadata_execution_table.dataset_id,
            'metadata_table_name': metadata_execution_table.table_id,
            'dataset': self.dataset,
            'table_name': self.table_name,
        }

    def prepare_template(self):
        operator_path = os.path.dirname(os.path.realpath(__file__))
        with open(f'{operator_path}/sql/check_metadata_execution.sql') as f:
            self.sql = f.read()
