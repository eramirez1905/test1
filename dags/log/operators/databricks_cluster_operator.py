from airflow.contrib.operators.databricks_operator import DatabricksHook, _deep_string_coerce
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

import time


GET_CLUSTER_ENDPOINT = ('GET', 'api/2.0/clusters/get')

CLUSTER_STATES = [
    'PENDING',          # Indicates that a cluster is in the process of being created
    'RUNNING',          # Indicates that a cluster has been started and is ready for use
    'RESTARTING',       # Indicates that a cluster is in the process of restarting
    'RESIZING',         # Indicates that a cluster is in the process of adding or removing nodes
    'TERMINATING',      # Indicates that a cluster is in the process of being destroyed
    'TERMINATED',       # Indicates that a cluster has been successfully destroyed
    'UNKNOWN'           # Indicates that a cluster is in an unknown state. A cluster should never be in this state
]


class DatabricksStartClusterOperator(BaseOperator):
    """
    Start an existing Spark cluster on Databricks using the
    `api/2.0/clusters/start` API endpoint.

    :param cluster_id: the cluster_id of the existing Databricks cluster.
        .. seealso::
            https://docs.databricks.com/api/latest/clusters.html#start
    :type cluster_id: str
    :param databricks_conn_id: The name of the Airflow connection to use.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    :type databricks_conn_id: str
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :type polling_period_seconds: int
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :type databricks_retry_limit: int
    """
    # Used in airflow.models.BaseOperator
    template_fields = ('json',)
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'

    @apply_defaults
    def __init__(
            self,
            cluster_id,
            databricks_conn_id='databricks_default',
            polling_period_seconds=30,
            databricks_retry_limit=3,
            databricks_retry_delay=1,
            **kwargs):

        super().__init__(**kwargs)
        self.json = {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay

        if cluster_id is not None:
            self.json['cluster_id'] = cluster_id

        self.json = _deep_string_coerce(self.json)

    def get_hook(self):
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay)

    def get_status(self):
        hook = self.get_hook()
        response = hook._do_api_call(GET_CLUSTER_ENDPOINT, self.json)

        if response['state'] not in CLUSTER_STATES:
            raise AirflowException(
                ('Unexpected state: {}: If the state has '
                 'been introduced recently, please check the Databricks user '
                 'guide for troubleshooting information').format(response['state']))

        return response['state']

    def execute(self, context):
        # hook.start_cluster fails on PENDING or RUNNING clusters (which can happen because we run multiple dags
        # on the same cluster). If cluster is PENDING we keep polling until state changes (if RUNNING we return early)
        polling_max_tries = 10
        polling_tries = 0
        while True:
            polling_tries += 1

            if polling_tries > polling_max_tries:
                break

            state = self.get_status()
            if state == 'PENDING':
                self.log.info('Cluster in state %s. Waiting for state to change (%s/%s)',
                              state, polling_tries, polling_max_tries)
                time.sleep(self.polling_period_seconds)
            elif state == 'RUNNING':
                self.log.info('Cluster already in state %s. %s completed successfully.',
                              state, self.task_id)
                return
            else:
                self.log.info('Cluster in state %s, but not PENDING or RUNNING, starting it.', state)
                break

        hook = self.get_hook()
        hook.start_cluster(self.json)

        while True:
            state = self.get_status()
            if state == 'UNKNOWN':
                raise AirflowException('Something horrible went down. Cluster is in {} state'.format(state))

            if state == 'RUNNING':
                self.log.info('%s completed successfully.', self.task_id)
                return
            else:
                self.log.info('Cluster is in {} state. Waiting for RUNNING state..'.format(state))
                time.sleep(self.polling_period_seconds)

    def on_kill(self):
        hook = self.get_hook()
        hook.terminate_cluster(self.json)
        self.log.info('Task: %s was requested to be cancelled. Terminating cluster', self.task_id)


class DatabricksTerminateClusterOperator(BaseOperator):
    """
    Terminate an existing Spark cluster on Databricks using the
    `api/2.0/clusters/delete` API endpoint.

    :param cluster_id: the cluster_id of the existing Databricks cluster.
        .. seealso::
            https://docs.databricks.com/api/latest/clusters.html#start
    :type cluster_id: str
    :param databricks_conn_id: The name of the Airflow connection to use.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    :type databricks_conn_id: str
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :type polling_period_seconds: int
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :type databricks_retry_limit: int
    """
    # Used in airflow.models.BaseOperator
    template_fields = ('json',)
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'

    @apply_defaults
    def __init__(
            self,
            cluster_id,
            databricks_conn_id='databricks_default',
            polling_period_seconds=30,
            databricks_retry_limit=3,
            databricks_retry_delay=1,
            **kwargs):

        super().__init__(**kwargs)
        self.json = {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay

        if cluster_id is not None:
            self.json['cluster_id'] = cluster_id

        self.json = _deep_string_coerce(self.json)

    def get_hook(self):
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay)

    def get_status(self):
        hook = self.get_hook()
        response = hook._do_api_call(GET_CLUSTER_ENDPOINT, self.json)

        if response['state'] not in CLUSTER_STATES:
            raise AirflowException(
                ('Unexpected state: {}: If the state has '
                 'been introduced recently, please check the Databricks user '
                 'guide for troubleshooting information').format(response['state']))

        return response['state']

    def execute(self, context):
        hook = self.get_hook()
        hook.terminate_cluster(self.json)

        while True:
            state = self.get_status()
            if state == 'UNKNOWN':
                raise AirflowException('Something horrible went down. Cluster is in {} state'.format(state))

            if state == 'TERMINATED':
                self.log.info('%s completed successfully.', self.task_id)
                return
            else:
                self.log.info('Cluster is in {} state. Waiting for TERMINATED state..'.format(state))
                time.sleep(self.polling_period_seconds)
