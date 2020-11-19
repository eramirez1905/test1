from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datahub.hooks.google_iam_hook import GoogleIamHook


class GoogleIamOperator(BaseOperator):
    ui_color = '#f0eee4'
    template_fields = ["project_id", "role_members"]

    @apply_defaults
    def __init__(
            self,
            task_id: str,
            project_id: str,
            role_members: list,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args, **kwargs):
        super().__init__(task_id=task_id, *args, **kwargs)

        self.delegate_to = delegate_to
        self.gcp_conn_id = gcp_conn_id
        self.role_members = role_members
        self.project_id = project_id
        self._hook = None

    def execute(self, context):
        if len(self.role_members) == 0:
            raise AirflowException("Task skipped because role_members is an empty array.")

        self.hook.update_google_iam_project_permissions(
            project_id=self.project_id,
            role_members=self._build_role_members()
        )

    def _build_role_members(self):
        users = []
        roles = {}
        for user in self.role_members:
            self._check_type(user)
            for role in user.get("roles", []):
                if role not in roles:
                    roles[role] = []
                roles[role].append(f'{user.get("type")}:{user.get("name")}')
        for role, members in roles.items():
            users.append({
                "members": members,
                "role": role
            })
        return users

    @staticmethod
    def _check_type(user):
        if user.get("type") not in ["user", "group", "serviceAccount"]:
            raise AirflowException(f'Invalid type: {user.get("type")} for user: {user.get("name")}')

    def _build_hook(self):
        return GoogleIamHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )

    @property
    def hook(self):
        if self._hook is None:
            self._hook = self._build_hook()
        return self._hook
