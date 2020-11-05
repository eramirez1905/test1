from datetime import datetime, timedelta

from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator

from datahub.common import alerts
from datahub.operators.kubernetes.kubernetes_pod_operator import KubernetesPodOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'concurrency': 1,
    'max_active_runs': 1,
    'executor_config': {
        'KubernetesExecutor': {
            'request_cpu': "200m",
            'limit_cpu': "200m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi",
        },
    },
    'start_date': datetime(2019, 4, 1),
    'end_date': None,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def simulate_failure():
    raise AirflowException('Task failed')


with DAG(dag_id='test-dag',
         description='Test DAG',
         schedule_interval=None,
         default_args=default_args,
         max_active_runs=1,
         catchup=False) as dag:

    test = PythonOperator(
        dag=dag,
        task_id='test-slack-notifications',
        on_failure_callback=alerts.setup_callback(),
        python_callable=simulate_failure
    )

    test_noop = PythonOperator(
        dag=dag,
        task_id='test-noop',
        python_callable=simulate_failure
    )

    test_kubernetes = KubernetesPodOperator(
        dag=dag,
        task_id=f'pod-test',
        name=f'test-kubernetes-pod',
        image='alpine:3.12',
        env_vars={
            'FOO': 'bar'
        },
        arguments=['sleep', '30'],
        team_name='data-engineering',
        service_name='airflow',
        tribe_name='data',
        resources={
            'request_cpu': "200m",
            'limit_cpu': "200m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi",
        }
    )
