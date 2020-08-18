from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.gcp_container_operator import GKEPodOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'testing_GKEPodOperator', default_args=default_args, schedule_interval=timedelta(minutes=10))

start = DummyOperator(task_id='run_this_first', dag=dag)

operator = GKEPodOperator(task_id='data-task',
                          project_id='peya-data-pocs',
                          location='us-central1-a',
                          cluster_name='spinnaker-1',
                          name='task-name',
                          namespace='airflow',
                          image='gcr.io/peya-data-pocs/data-analysts-toolkit@sha256:da37ac9704179e28f17336f187ba3ddee4005883d736801f5201283fe5e4e62c')

operator.set_upstream(start)
