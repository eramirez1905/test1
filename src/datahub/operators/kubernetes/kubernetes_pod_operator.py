import os

from airflow import conf, DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator as BaseKubernetesPodOperator
from airflow.utils.decorators import apply_defaults


class KubernetesPodOperator(BaseKubernetesPodOperator):
    """
    Execute a task in a Kubernetes Pod with preconfigured defaults configuration

    :param keep_pod_on_extended_retries: Debug parameter that keeps a pod around if it is being retried more than
        its set configuration to help with logs
    :type keep_pod_on_extended_retries: bool
    :param team_name: Team name used to track costs (e.g. data-engineering)
    :type team_name: str
    :param service_name: Service name used to track costs (e.g. airflow)
    :type service_name: str
    :param tribe_name: Tribe name used to track costs ( e.g. data)
    :type tribe_name: str
    :param tribe_name: Unit name used to track costs ( e.g. logistics, pandata, mkt, dmart)
        By default is $AIRFLOW_BUSINESS_UNIT
    :type tribe_name: str

    """

    @apply_defaults
    def __init__(self,
                 dag: DAG,
                 task_id: str,
                 image: str,
                 name: str,
                 team_name,
                 service_name,
                 tribe_name,
                 unit_name=os.getenv('AIRFLOW_BUSINESS_UNIT'),
                 namespace=None,
                 keep_pod_on_extended_retries=False,
                 is_delete_operator_pod=True,
                 labels=None,
                 in_cluster=conf.getboolean('kubernetes', 'in_cluster'),
                 image_pull_policy=conf.get('kubernetes', 'worker_container_image_pull_policy'),
                 startup_timeout_seconds=300,  # Default timeout to 5 minutes, not 2
                 do_xcom_push=False,
                 *args, **kwargs):
        if namespace is None:
            namespace = self.__get_namespace()
        super().__init__(
            task_id=task_id,
            dag=dag,
            namespace=namespace,
            image=image,
            name=name,
            is_delete_operator_pod=is_delete_operator_pod,
            labels=labels,
            in_cluster=in_cluster,
            image_pull_policy=image_pull_policy,
            startup_timeout_seconds=startup_timeout_seconds,
            do_xcom_push=do_xcom_push,
            *args, **kwargs)

        self.keep_pod_on_extended_retries = keep_pod_on_extended_retries
        self.team_name = team_name
        self.service_name = service_name
        self.tribe_name = tribe_name
        self.unit_name = unit_name

    @staticmethod
    def __get_namespace():
        namespace = conf.get('kubernetes', 'namespace')
        if namespace == 'logistics':
            namespace = 'log'
        return namespace

    def pre_execute(self, context):
        # All arguments passed to a pod must be strings
        self.arguments = [str(a) for a in self.arguments]

        execution_date = context['execution_date'].isoformat().replace('+', '_plus_').replace(":", "_")
        self.labels.update({
            'task_id': context['ti'].task_id,
            'dag_id': context['dag'].dag_id,
            'execution_date': execution_date,
            'country': os.getenv('AIRFLOW_BUSINESS_UNIT'),
            'business_unit': os.getenv('AIRFLOW_BUSINESS_UNIT'),
            'unit': self.unit_name,
            'service': self.service_name,
            'team': self.team_name,
            'tribe': self.tribe_name,
        })

        # Increase number of boto tries,
        # Default is 1 attempt with 1 second timeout
        self.env_vars['AWS_METADATA_SERVICE_NUM_ATTEMPTS'] = '5'
        self.env_vars['AWS_METADATA_SERVICE_TIMEOUT'] = '25'

        # Pass CPU & memory request and limit values to container to know how much power is available at runtime
        self.env_vars['LIMIT_CPU'] = self.resources.limit_cpu
        self.env_vars['REQUEST_CPU'] = self.resources.request_cpu
        self.env_vars['LIMIT_MEMORY'] = self.resources.limit_memory
        self.env_vars['REQUEST_MEMORY'] = self.resources.request_memory

        # Pass info about DAG ID, Task ID and business unit to container for BigQuery labelling
        self.env_vars['AIRFLOW_DAG_ID'] = context['dag'].dag_id
        self.env_vars['AIRFLOW_TASK_ID'] = context['ti'].task_id
        self.env_vars['AIRFLOW_BUSINESS_UNIT'] = os.getenv('AIRFLOW_BUSINESS_UNIT')

        if self.keep_pod_on_extended_retries and (context['task_instance'].try_number > self.retries + 1):
            self.is_delete_operator_pod = False
            self.log.warning("Extended retries enabled, pod will not be deleted in this case")
