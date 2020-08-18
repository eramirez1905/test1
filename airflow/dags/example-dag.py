from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import GKEPodOperator

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

passing = GKEPodOperator(namespace='airflow',
                          image="gcr.io/peya-data-pocs/data-analysts-toolkit@sha256:da37ac9704179e28f17336f187ba3ddee4005883d736801f5201283fe5e4e62c",
                        #   cmds=["python","-c"],
                        #   arguments=["print('hello world')"],
                          labels={"data": "analitycs"},
                          name="data-test",
                          task_id="data-task",
                          get_logs=True,
                          dag=dag,
                          in_cluster=True
                          )

passing.set_upstream(start)
