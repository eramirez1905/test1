from airflow.models import BaseOperator

from datahub.hooks.bigquery_hook import BigQueryHook


class BigQueryPolicyTagsOperator(BaseOperator):

    template_fields = ['project_id', 'dataset_id', 'table_id', 'columns']

    def __init__(self,
                 dataset_id: str,
                 table_id: str,
                 project_id: str,
                 policy_tags: list,
                 columns: list,
                 bigquery_conn_id: str = 'bigquery_default',
                 delegate_to: str = None,
                 is_enabled: bool = True,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.project_id = project_id
        self.policy_tags = policy_tags
        self.columns = columns
        self.is_enabled = is_enabled
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self._hook = None

    def execute(self, context):
        if not self.is_enabled:
            self.log.info(f'Task "{self.task_id}" is disabled')
            return

        self.log.debug("policy_tags %s", self.policy_tags)
        policy_tags = {policy.get("display_name"): policy.get("name") for policy in self.policy_tags}
        columns = {row.get("column"): row.get("tag") for row in self.columns}

        self.hook.update_policy_tags(
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            project_id=self.project_id,
            policy_tags=policy_tags,
            columns=columns,
        )

    @property
    def hook(self):
        if self._hook is None:
            self._hook = BigQueryHook(
                bigquery_conn_id=self.bigquery_conn_id,
                delegate_to=self.delegate_to
            )
        return self._hook
