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

dag = DAG('test_kubernetes_pod',
          schedule_interval='@once',
          default_args=default_args)

with dag:
    k = KubernetesPodOperator(
        namespace="airflow",
        image="ubuntu",
        labels={"foo": "bar"},
        name="airflow-test-pod",
        task_id="task-one",
        get_logs=True)