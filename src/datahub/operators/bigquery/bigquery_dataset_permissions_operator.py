from typing import List, Dict

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datahub.hooks.bigquery_hook import BigQueryHook


class BigQueryDatasetPermissionsOperator(BaseOperator):
    template_fields = ['project_id', 'dataset_id', 'access_entries']
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
            self,
            task_id: str,
            project_id: str,
            dataset_id: str,
            access_entries=None,
            bigquery_conn_id='bigquery_default',
            delegate_to=None,
            *args, **kwargs):
        super().__init__(task_id=task_id, *args, **kwargs)

        self.delegate_to = delegate_to
        self.bigquery_conn_id = bigquery_conn_id
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.access_entries = access_entries or []
        self._hook = None

    def execute(self, context):
        blacklist_roles = []
        access_entries = self.__extract_access_entries()
        self.__validate_curated_data_datasets_to_have_proper_roles(access_entries=access_entries)

        self.hook.update_google_iam_dataset_permissions(
            dataset_id=self.dataset_id,
            project_id=self.project_id,
            access_entries=access_entries,
            blacklist_roles=blacklist_roles,
        )

    def _build_hook(self) -> BigQueryHook:
        return BigQueryHook(
            bigquery_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to
        )

    @property
    def hook(self) -> BigQueryHook:
        if self._hook is None:
            self._hook = self._build_hook()
        return self._hook

    def __validate_curated_data_datasets_to_have_proper_roles(self, access_entries):
        if self.dataset_id.startswith('curated_data_'):
            blacklist_roles = ['READER']
            if len([entry for entry in access_entries if entry.get("role") in blacklist_roles]) > 0:
                raise AirflowException(f'Roles {",".join(blacklist_roles)} are not allowed for the dataset {self.dataset_id}')

    def __extract_access_entries(self) -> List[Dict[str, str]]:
        access_entries = []
        for role, access_entry in self.access_entries.items():
            for kind, emails in access_entry.items():
                for email in emails:
                    access_entries.append({
                        "role": role,
                        kind: email,
                    })
        return access_entries
