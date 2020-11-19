import itertools

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datahub.hooks.bigquery_hook import BigQueryHook


class BigQueryPermissionsOperator(BaseOperator):
    template_fields = ('dataset_id', 'acl')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
            self,
            task_id,
            dataset_id,
            acl=None,
            bigquery_conn_id='bigquery_default',
            delegate_to=None,
            *args, **kwargs):
        super().__init__(task_id=task_id, *args, **kwargs)

        self.delegate_to = delegate_to
        self.bigquery_conn_id = bigquery_conn_id
        self.dataset_id = dataset_id
        self.acl = acl or []
        self._hook = None

    def execute(self, context):
        users = list(itertools.chain.from_iterable([e.get('users', []) for e in self.acl]))
        email_addresses = [e.get('email') for e in users if e.get('email')]
        group_addresses = [e.get('group') for e in users if e.get('group')]

        self.hook.update_data_viewer_permissions(
            dataset_id=self.dataset_id,
            email_addresses=email_addresses,
            group_addresses=group_addresses
        )

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
