from datetime import timedelta, datetime

from airflow import DAG, configuration
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from configuration import config
from datahub.common import alerts
from datahub.operators.sql_s3_operator import SqlToS3Operator
from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator

default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime(2018, 8, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'executor_config': {
        'KubernetesExecutor': {
            'request_cpu': "200m",
            'limit_cpu': "200m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi",
        },
    },
}

pod_resources = {
    'request_cpu': "500m",
    'limit_cpu': "500m",
    'request_memory': "6000Mi",
    'limit_memory': "6000Mi",
}

s3_bucket = Variable.get('data-science-data-lake-bucket',
                         default_var='production-eu-data-science-data-lake')
env_vars = {
    'AERIS_ENV_ACCESS_ID': Variable.get('aeris-env-access-id', default_var="None"),
    'AERIS_ENV_SECRET_KEY': Variable.get('aeris-env-secret-key', default_var="None"),
}

s3_download_path = "raw_data/weather_data/json"
bucket_args = ["-b", s3_bucket, "-k", s3_download_path]

dags_folder = configuration.get('core', 'dags_folder')
data_lake_bucket = Variable.get('data-science-data-lake-bucket', default_var='production-eu-data-science-data-lake')
S3_FEATURE_OUTPUT_STORE_PATH = 'feature_store/weather_data'

dag = DAG(
    dag_id=f'download-weather-data',
    description='download-weather-data',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 13 * * *',
    template_searchpath=f'{dags_folder}/forecast/sql',
    on_failure_callback=alerts.setup_callback(),
)


forecast_operator = LogisticsKubernetesOperator(
    image="683110685365.dkr.ecr.eu-west-1.amazonaws.com/download-weather",
    env_vars=env_vars,
    cmds=["python"],
    arguments=["download_weather.py", "-t", "forecast"] + bucket_args,
    name="download-weather-forecast",
    task_id="download-weather-forecast-container",
    resources=pod_resources,
    dag=dag
)

forecast_operator_icon = LogisticsKubernetesOperator(
    image="683110685365.dkr.ecr.eu-west-1.amazonaws.com/download-weather",
    env_vars=env_vars,
    cmds=["python"],
    arguments=["download_weather.py", "-t", "forecast", "-w", "ICON"] + bucket_args,
    name="download-weather-forecast-icon",
    task_id="download-weather-forecast-icon-container",
    resources=pod_resources,
    dag=dag
)

forecast_operator_dwd = LogisticsKubernetesOperator(
    image="683110685365.dkr.ecr.eu-west-1.amazonaws.com/download-weather",
    env_vars=env_vars,
    cmds=["python"],
    arguments=["download_weather.py", "-t", "forecast", "-w", "DWD"] + bucket_args,
    name="download-weather-forecast-dwd",
    task_id="download-weather-forecast-dwd-container",
    resources=pod_resources,
    dag=dag
)

historic_operator = LogisticsKubernetesOperator(
    image="683110685365.dkr.ecr.eu-west-1.amazonaws.com/download-weather",
    env_vars=env_vars,
    cmds=["python"],
    arguments=["download_weather.py", "-t", "historical", "-s", "20150101"] + bucket_args,
    name="download-weather-history",
    task_id="download-weather-history-container",
    trigger_rule="all_done",
    resources=pod_resources,
    dag=dag
)

historic_operator_dwd = LogisticsKubernetesOperator(
    image="683110685365.dkr.ecr.eu-west-1.amazonaws.com/download-weather",
    env_vars=env_vars,
    cmds=["python"],
    arguments=["download_weather.py", "-t", "historical", "-w", "DWD"] + bucket_args,
    name="download-weather-history-dwd",
    task_id="download-weather-history-dwd-container",
    trigger_rule="all_done",
    resources=pod_resources,
    dag=dag,
    on_success_callback=alerts.setup_callback()
)


def create_weather_feature_store_operator(parent_dag, country, data_source):
    """Create tasks for feature data uploads."""
    s3_dataset_path = S3_FEATURE_OUTPUT_STORE_PATH + "/{{ds}}/" + f"{country}/weather_{data_source}.csv.gz"
    feature_store_operator = SqlToS3Operator(
        task_id=f'write_weather_{data_source}_{country}',
        sql=f'weather.sql',
        pg_connection_id='dwh',
        s3_bucket_name=data_lake_bucket,
        s3_key=s3_dataset_path,
        params={'country_code': country, 'data_source': data_source},
        separator=";",
        pool=config['pools']['process_order_forecast']['name'],
        execution_timeout=timedelta(hours=2),
        dag=parent_dag
    )
    return feature_store_operator


def process_country(parent_dag, country, start, end):

    write_weather_AERIS_to_feature_store_operator = create_weather_feature_store_operator(parent_dag, country, 'AERIS')
    write_weather_DWD_to_feature_store_operator = create_weather_feature_store_operator(parent_dag, country, 'DWD')
    write_weather_ICON_to_feature_store_operator = create_weather_feature_store_operator(parent_dag, country, 'ICON')

    (start
     >> write_weather_AERIS_to_feature_store_operator
     >> write_weather_DWD_to_feature_store_operator
     >> write_weather_ICON_to_feature_store_operator
     >> end)


start = DummyOperator(
    dag=dag,
    task_id='start'
)

middle = DummyOperator(
    dag=dag,
    task_id='middle'
)

end = DummyOperator(
    dag=dag,
    task_id='end'
)

# Order matters, as we can run out of api calls due to a back fill of historic data but still want to have forecast data
# for other cities
start >> forecast_operator >> historic_operator >> middle
start >> forecast_operator_icon >> middle
start >> forecast_operator_dwd >> historic_operator_dwd >> middle

for country in sorted([cc for region in config['regions'] for cc in config['regions'][region]['countries']]):
    process_country(dag, country, middle, end)
