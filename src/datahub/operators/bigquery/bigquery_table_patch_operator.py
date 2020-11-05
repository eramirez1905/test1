from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datahub.hooks.bigquery_hook import BigQueryHook


class BigQueryTablePatchOperator(BaseOperator):

    template_fields = ('dataset_id', 'table_id', 'require_partition_filter')
    ui_color = '#a3d6f1'

    @apply_defaults
    def __init__(
            self,
            task_id,
            dataset_id,
            table_id,
            enabled=True,
            require_partition_filter=None,
            bigquery_conn_id='bigquery_default',
            delegate_to=None,
            *args, **kwargs):
        super().__init__(task_id=task_id, *args, **kwargs)

        self.delegate_to = delegate_to
        self.bigquery_conn_id = bigquery_conn_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.is_enabled = enabled
        self.require_partition_filter = require_partition_filter
        self._hook = None
        if not self.is_enabled:
            self.ui_color = '#000'
            self.ui_fgcolor = '#FFF'

    def _build_hook(self):
        return BigQueryHook(
            bigquery_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to
        )

    @property
    def hook(self):
        if self._hook is None:
            self._hook = self._build_hook()
        return self._hook

    def execute(self, context):
        if not self.is_enabled:
            self.log.info(f'Task "{self.task_id}" is disabled')
            return

        self.hook.get_cursor().patch_table(
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            require_partition_filter=self.require_partition_filter
        )
