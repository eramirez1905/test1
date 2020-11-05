from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from configuration import config
from datahub.common import alerts
from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator

image_version = Variable.get('no-show-forecast-version', default_var='latest')
image = f'683110685365.dkr.ecr.eu-west-1.amazonaws.com/no-show-forecast-models:{image_version}'

default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=4),
    'executor_config': {
        'KubernetesExecutor': {
            'request_cpu': "200m",
            'limit_cpu': "200m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi",
        },
    },
}

# An empty default var means that countries in that region will not be uploaded
S3_BASE_KEY_PATH = 'no_shows/{{ds}}/'

env_vars = {
    "FORECAST_API_TOKEN": Variable.get("hurrier-api-token", default_var=""),
}

country_codes = sorted([cc for region in config['regions']
                        for cc in config['regions'][region]['countries']])
large_cc = ['se', 'ar', 'ae', 'ph', 'sa', 'sg', 'tw', 't3']
extra_large_cc = ['my', 'th']

norm_process_resources = {
    'request_cpu': '1500m',
    'limit_cpu': '1500m',
    'request_memory': '6000Mi',
    'limit_memory': '6000Mi',
}
large_process_resources = {
    'request_cpu': '2500m',
    'limit_cpu': '2500m',
    'request_memory': '9500Mi',
    'limit_memory': '9500Mi',
}
extra_large_process_resources = {
    'request_cpu': '2500m',
    'limit_cpu': '2500m',
    'request_memory': '30000Mi',
    'limit_memory': '30000Mi',
}

dag = DAG(
    dag_id=f'upload-no-show-forecast-v1',
    description='Upload no show forecast',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 18 * * *',
    tags=[default_args['owner']],
)

start = DummyOperator(dag=dag, task_id='start')
end = DummyOperator(dag=dag, task_id='end')

query_data_op = LogisticsKubernetesOperator(
    task_id=f'all-query-no-show-data',
    name=f'all-query-no-show-data',
    image=image,
    env_vars=env_vars,
    cmds=['python'],
    arguments=['scripts/query_data.py', '-s', '{{macros.ds_add(ds, -200)}}'],
    resources={
        'request_cpu': '1000m',
        'limit_cpu': '1000m',
        'request_memory': '2000Mi',
        'limit_memory': '20000Mi',
    },
    dag=dag
)

start >> query_data_op

combine_data_op = LogisticsKubernetesOperator(
    task_id=f'combine-no-show-data',
    name=f'combine-no-show-data',
    image=image,
    env_vars=env_vars,
    node_profile='simulator-node',
    cmds=['python'],
    arguments=['scripts/combine_data.py', '-k', S3_BASE_KEY_PATH, '-t', '{{ds}}'],
    resources={
        'request_cpu': '1000m',
        'limit_cpu': '1500m',
        'request_memory': '16000Mi',
        'limit_memory': '16000Mi',
    },
    dag=dag
)

for country_code in country_codes:
    s3_country_path = S3_BASE_KEY_PATH + country_code + "/"

    train_resources = norm_process_resources
    if country_code in large_cc:
        train_resources = large_process_resources
    if country_code in extra_large_cc:
        train_resources = extra_large_process_resources

    process_data_op = LogisticsKubernetesOperator(
        task_id=f'process-no-show-data-{country_code}',
        name=f'process-no-show-data-{country_code}',
        image=image,
        env_vars=env_vars,
        cmds=['python'],
        arguments=['scripts/process_data.py', '-k', s3_country_path, '-c', country_code,
                   '-r', '{{macros.ds_add(ds, -1)}}'],
        node_profile='simulator-node',
        resources=train_resources,
        dag=dag
    )

    train_model_op = LogisticsKubernetesOperator(
        task_id=f'train-no-show-model-{country_code}',
        name=f'train-no-show-model-{country_code}',
        image=image,
        env_vars=env_vars,
        node_profile='simulator-node',
        cmds=['python'],
        arguments=['scripts/train_models.py', '-c', country_code, '-k', s3_country_path],
        resources=train_resources,
        dag=dag
    )

    upload_prediction_op = LogisticsKubernetesOperator(
        task_id=f'upload-no-show-prediction-{country_code}',
        name=f'upload-no-show-prediction-{country_code}',
        image=image,
        env_vars=env_vars,
        node_profile='simulator-node',
        cmds=['python'],
        arguments=['scripts/upload_prediction.py', '-c', country_code, '-k', s3_country_path],
        resources={
            'request_cpu': '1000m',
            'limit_cpu': '1000m',
            'request_memory': '15000Mi',
            'limit_memory': '15000Mi',
        },
        dag=dag,
        on_success_callback=alerts.setup_callback(),
        on_failure_callback=alerts.setup_callback(),
    )
    query_data_op >> process_data_op >> train_model_op >> upload_prediction_op >> combine_data_op

combine_data_op >> end
