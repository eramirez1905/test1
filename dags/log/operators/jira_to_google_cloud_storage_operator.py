import tempfile

import tenacity
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.jira_hook import JiraHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from atlassian.jira_json_cleansing import JiraJsonCleansing


class JiraToGcsOperator(BaseOperator):
    template_fields = ['gcs_object_name', 'jira_project', 'issue_fields']

    @apply_defaults
    def __init__(self,
                 jira_project,
                 gcs_bucket_name,
                 gcs_object_name,
                 issue_fields,
                 jira_conn_id='jira_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 mime_type='text/json',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._hook = None
        self.jira_project = jira_project
        self.jira_conn_id = jira_conn_id
        self.issue_fields = ",".join(issue_fields)

        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_object_name = gcs_object_name
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.retry_args = dict(
            wait=tenacity.wait_exponential(multiplier=2),
            stop=tenacity.stop_after_attempt(3),
            retry=tenacity.retry_if_exception_type(Exception),
        )

    def execute(self, context):
        with tempfile.NamedTemporaryFile(delete=False, mode='a+') as tmp_file:
            try:
                for issues in self._fetch_issues(self.hook.client, self.jira_project, context['ts']):
                    tmp_file.write(issues + "\n")
            except Exception as e:
                raise AirflowException(f'Jira API error: {str(e)}')

            self._upload_to_gcs(tmp_file)
            self.log.info(f'Upload successful: {self.gcs_bucket_name}/{self.gcs_object_name}')

    def _fetch_issues(self, jira_hook, project_id, execution_date):
        jira_json_cleansing = JiraJsonCleansing(execution_date)
        start_at = 0
        max_results = 1000  # The API returns a maximum of 1000 records in a single call
        total_issues = 1
        while start_at < total_issues:
            issues = jira_hook.search_issues(jql_str=f"project={project_id}", startAt=start_at, maxResults=max_results)
            for issue in issues:
                issue = jira_hook.issue(id=issue.id, fields=self.issue_fields)
                issue_json = jira_json_cleansing.prepare(issue)
                yield issue_json
            start_at += len(issues)
            total_issues = issues.total
        self.log.info(f'Successfully fetched {total_issues} issues from JIRA for {project_id}')

    def _build_hook(self):
        return JiraHook(jira_conn_id=self.jira_conn_id)

    @property
    def hook(self):
        if self._hook is None:
            self._hook = self._build_hook()
        return self._hook

    def _upload_to_gcs(self, tmp_file):
        # Hook to upload the payload to google cloud storage
        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )
        self.log.info(f'Upload to {self.gcs_bucket_name}')
        gcs_hook.upload(
            bucket=self.gcs_bucket_name,
            object=f'{self.gcs_object_name}',
            filename=tmp_file.name,
            mime_type=self.mime_type
        )
