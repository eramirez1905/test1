from datetime import datetime

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datahub.curated_data.metadata_execution_table import MetadataExecutionTable
from datahub.hooks.bigquery_hook import BigQueryHook


class BigQueryUpdateMetadataExecutionOperator(BaseOperator):
    ui_color = '#6ccac9'
    ui_fgcolor = '#FFF'

    template_fields = ['project_id', 'dataset_id', 'table_name', 'backfill_to_date', 'is_shared', 'json']

    @apply_defaults
    def __init__(self,
                 task_id,
                 project_id: str,
                 dataset_id: str,
                 table_name: str,
                 metadata_execution_table: MetadataExecutionTable,
                 backfill_to_date: str = None,
                 is_shared: bool = False,
                 bigquery_conn_id: str = 'bigquery_default',
                 *args,
                 **kwargs):
        super().__init__(task_id=task_id, *args, **kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.metadata_execution_table = metadata_execution_table
        self.is_shared = is_shared
        self.bigquery_conn_id = bigquery_conn_id
        self._hook = None
        self.backfill_to_date = backfill_to_date
        self.json = None

    def prepare_template(self):
        self.json = self.payload

    def execute(self, context):
        self.hook.get_cursor().insert_all(
            self.project_id,
            self.metadata_execution_table.dataset_id,
            self.metadata_execution_table.table_id,
            [{"json": self.json}],
            fail_on_error=True
        )

    def get_to_date(self) -> str:
        return str(self.backfill_to_date if self.backfill_to_date else "{{next_execution_date}}")

    @property
    def payload(self) -> dict:
        return {
            "dag_id": self.dag.dag_id,
            "dataset": self.dataset_id,
            "table_name": self.table_name,
            "is_shared": self.is_shared,
            "execution_date": "{{execution_date}}",
            "next_execution_date": self.get_to_date(),
            "row_count": None,
            "size_bytes": None,
            "last_modified_time": None,
            "_ingested_at": self.get_utc_now()
        }

    @staticmethod
    def get_utc_now() -> str:
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    @property
    def hook(self) -> BigQueryHook:
        if self._hook is None:
            self._hook = BigQueryHook(
                self.bigquery_conn_id,
                use_legacy_sql=False
            )
        return self._hook
