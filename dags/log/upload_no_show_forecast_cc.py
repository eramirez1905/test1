from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from configuration import config
from datahub.common import alerts
from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator

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

train_resources = {
    'request_cpu': '1500m',
    'limit_cpu': '1500m',
    'request_memory': '6000Mi',
    'limit_memory': '6000Mi',
}

S3_BASE_KEY_PATH = 'no_shows/contact_center/{{ds}}/'

DATA_LAKE_BUCKET = Variable.get('no-show-forecast-data-lake-bucket',
                                default_var='production-eu-data-science-data-lake')

MODEL_OUTPUT_BUCKET = Variable.get('no-show-forecast-model-output-bucket',
                                   default_var='production-eu-data-science-model-outputs')

env_vars = {
    "FORECAST_API_TOKEN": Variable.get("hurrier-api-token", default_var=""),
    "DEFAULT_TEMP_GCS_BUCKET": DATA_LAKE_BUCKET
}

image_version = Variable.get('no-show-forecast-version', default_var='latest')
no_show_image = f'683110685365.dkr.ecr.eu-west-1.amazonaws.com/no-show-forecast-models:{image_version}'

# list of contact center rooster environments
# all contact center rooster envs. are in region cc-eu
rooster_environments = config['contact_center']['regions']['cc-eu']['environments']

dag = DAG(
    dag_id=f'upload-no-show-forecast-contact-center-v1',
    description='Upload no show forecast for contact centers',
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
    image=no_show_image,
    env_vars=env_vars,
    cmds=['python'],
    arguments=['scripts/query_data.py',
               # start_date
               '-s', '{{macros.ds_add(ds, -200)}}',
               # is_contact-center
               '-c', 'true'],
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
    image=no_show_image,
    env_vars=env_vars,
    node_profile='simulator-node',
    cmds=['python'],
    arguments=['scripts/combine_data.py',
               # Model output bucket
               '-m', MODEL_OUTPUT_BUCKET,
               # S3 folder where results are stored
               '-k', S3_BASE_KEY_PATH,
               # train date
               '-t', '{{ds}}'],
    resources={
        'request_cpu': '1000m',
        'limit_cpu': '1500m',
        'request_memory': '16000Mi',
        'limit_memory': '16000Mi',
    },
    dag=dag
)

for environment in rooster_environments:
    s3_country_path = S3_BASE_KEY_PATH + environment + "/"

    process_data_op = LogisticsKubernetesOperator(
        task_id=f'process-no-show-data-{environment}',
        name=f'process-no-show-data-{environment}',
        image=no_show_image,
        env_vars=env_vars,
        cmds=['python'],
        arguments=['scripts/process_data.py',
                   # S3 folder where results are stored
                   '-k', s3_country_path,
                   # rooster environment
                   '-c', environment,
                   # run date
                   '-r', '{{macros.ds_add(ds, -1)}}',
                   # data lake bucket
                   '-d', DATA_LAKE_BUCKET],
        node_profile='simulator-node',
        resources=train_resources,
        dag=dag
    )

    train_model_op = LogisticsKubernetesOperator(
        task_id=f'train-no-show-model-{environment}',
        name=f'train-no-show-model-{environment}',
        image=no_show_image,
        env_vars=env_vars,
        node_profile='simulator-node',
        cmds=['python'],
        arguments=['scripts/train_models.py',
                   # rooster environment
                   '-c', environment,
                   # S3 folder where results are stored
                   '-k', s3_country_path,
                   # data lake bucket
                   '-d', DATA_LAKE_BUCKET,
                   # model output bucket
                   '-m', MODEL_OUTPUT_BUCKET],
        resources=train_resources,
        dag=dag
    )

    upload_prediction_op = LogisticsKubernetesOperator(
        task_id=f'upload-no-show-prediction-{environment}',
        name=f'upload-no-show-prediction-{environment}',
        image=no_show_image,
        env_vars=env_vars,
        node_profile='simulator-node',
        cmds=['python'],
        arguments=['scripts/upload_prediction.py',
                   # rooster environment
                   '-c', environment,
                   # S3 folder where results are stored
                   '-k', s3_country_path,
                   # model output bucket
                   '-m', MODEL_OUTPUT_BUCKET],
        resources={
            'request_cpu': '1000m',
            'limit_cpu': '1000m',
            'request_memory': '8000Mi',
            'limit_memory': '8000Mi',
        },
        dag=dag,
        on_success_callback=alerts.setup_callback(),
        on_failure_callback=alerts.setup_callback(),
    )
    query_data_op >> process_data_op >> train_model_op >> upload_prediction_op >> combine_data_op

combine_data_op >> end
