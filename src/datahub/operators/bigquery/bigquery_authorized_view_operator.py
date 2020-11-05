from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datahub.hooks.bigquery_hook import BigQueryHook


class BigQueryAuthorizeViewOperator(BaseOperator):
    template_fields = ('source_dataset', 'source_project', 'view_dataset', 'view_table', 'view_project')
    ui_color = '#1dc2eb'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(
            self,
            task_id,
            source_dataset,
            view_dataset,
            view_table,
            source_project=None,
            view_project=None,
            bigquery_conn_id='bigquery_default',
            delegate_to=None,
            *args, **kwargs):
        super().__init__(task_id=task_id, *args, **kwargs)

        self.delegate_to = delegate_to
        self.bigquery_conn_id = bigquery_conn_id
        self.source_dataset = source_dataset
        self.view_dataset = view_dataset
        self.view_table = view_table
        self._hook = None
        self.source_project = source_project
        self.view_project = view_project

    def execute(self, context):
        self.hook.get_cursor().run_grant_dataset_view_access(self.source_dataset, self.view_dataset, self.view_table,
                                                             self.source_project, self.view_project)

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
