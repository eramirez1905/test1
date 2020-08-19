from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('example_kubernetes_pod',
          schedule_interval='@once',
          default_args=default_args)

with dag:
    k = KubernetesPodOperator(
        namespace="airflow",
        image="hello-world",
        labels={"foo": "bar"},
        name="airflow-test-pod",
        task_id="task-one",
        in_cluster=False, # if set to true, will look in the cluster, if false, looks for file
        config_file=None,
        is_delete_operator_pod=True,
        get_logs=True)