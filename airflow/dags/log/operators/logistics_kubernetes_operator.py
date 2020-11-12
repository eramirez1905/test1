from airflow.contrib.hooks.databricks_hook import DatabricksHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.utils.decorators import apply_defaults

from configuration import config
from datahub.operators.kubernetes.kubernetes_pod_operator import KubernetesPodOperator


class LogisticsKubernetesOperator(KubernetesPodOperator):
    """
    Execute a task in a Kubernetes Pod with preconfigured defaults to run on logistics kubernetes configuration

    :param node-profile: What node to run pod on, 'default-node' to use standard dwh ones
    :type node-profile: str
    :param google_conn_id: Adds a google connection id authentication json as an env param to the pod
    :type google_conn_id: str
    :param databricks_conn_id: Adds a Databricks connection id authentication as an env param to the pod
    :type databricks_conn_id: str

    """

    @apply_defaults
    def __init__(self,
                 task_id,
                 dag,
                 node_profile='simulator-node',
                 namespace='dwh',
                 startup_timeout_seconds=1200,  # Default timeout to 20 minutes
                 google_conn_id='bigquery_default',
                 databricks_conn_id='databricks_default',
                 affinity=None,
                 tolerations=None,
                 team_name='data-science',
                 service_name='airflow',
                 tribe_name='data',
                 step_name=None,
                 *args,
                 **kwargs):
        super().__init__(
            task_id=task_id,
            dag=dag,
            namespace=namespace,
            startup_timeout_seconds=startup_timeout_seconds,
            affinity=affinity or config[node_profile]['affinity'],
            tolerations=tolerations or config[node_profile]['tolerations'],
            team_name=team_name,
            service_name=service_name,
            tribe_name=tribe_name,
            *args,
            **kwargs)

        self.google_conn_id = google_conn_id
        self.databricks_conn_id = databricks_conn_id
        self.step_name = step_name

    def pre_execute(self, context):
        super().pre_execute(context=context)

        self.labels.update({
            'app': 'airflow_kubernetes_operator',
        })

        if self.google_conn_id:
            google_conn = GoogleCloudBaseHook(self.google_conn_id)
            self.env_vars['GOOGLE_APPLICATION_CREDENTIALS_JSON'] = google_conn.extras['extra__google_cloud_platform__keyfile_dict']

        if self.databricks_conn_id:
            conn = DatabricksHook(self.databricks_conn_id)
            if 'token' in conn.databricks_conn.extra_dejson:
                self.env_vars['DATABRICKS_API_TOKEN'] = conn.databricks_conn.extra_dejson['token']
            if 'host' in conn.databricks_conn.extra_dejson:
                self.env_vars['DATABRICKS_ADDRESS'] = f"https://{conn.databricks_conn.extra_dejson['host']}"
