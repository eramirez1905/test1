import json
from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable

from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator

default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 4),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'executor_config': {
        'KubernetesExecutor': {
            'request_cpu': "200m",
            'limit_cpu': "200m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi",
        }
    }
}

tests_configs = Variable.get('delivery-areas-optimization-aa-tests-configs', default_var=[], deserialize_json=True)
env_variables = Variable.get('delivery-areas-optimization-aa-tests-env-variables', default_var={}, deserialize_json=True)

# 4 AM in SG
# 1 AM in PK
schedule = '0 20 * * *'

VERSION = 2
IMAGE_TAG = env_variables['image'] if 'image' in env_variables else 'latest'
DOCKER_IMAGE = f'683110685365.dkr.ecr.eu-west-1.amazonaws.com/delivery-areas-optimization:{IMAGE_TAG}'

dag = DAG(
    dag_id=f'da-optimization-aa-tests-v{VERSION}',
    description='Daily switch between different DA setups',
    default_args=default_args,
    catchup=False,
    schedule_interval=schedule,
    tags=[default_args['owner']],
)


if len(tests_configs) > 0:
    da_setup_switcher_operator = LogisticsKubernetesOperator(
        task_id=f'da-optimization-aa-tests-switcher',
        name=f'da-optimization-aa-tests-switcher',
        image=DOCKER_IMAGE,
        cmds=['python3.7', 'aa_testing/switch_da_setups.py'],
        node_profile='simulator-node',
        env_vars=env_variables,
        arguments=[
            '--execution-date', '{{next_ds}}',
            '--test-configs', json.dumps(tests_configs)
        ],
        resources={
            'request_cpu': "200m",
            'limit_cpu': "200m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi"
        },
        dag=dag
    )
