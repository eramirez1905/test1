from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from configuration import config
from forecast.operators import create_order_forecast_operator, create_forecast_exists_check_operator, \
    create_s3_to_gcs_operator, create_gcs_to_bq_operator, create_bq_table_operator

from forecast.utils import get_countries

version = 1
dags_folder = configuration.get('core', 'dags_folder')
schedule = '0 18 * * *'

default_args = {
    'owner': 'data-science',
    'start_date': datetime(2019, 2, 17),
    'retries': 2,
    # this dag is not time critical, but somewhat higher delays can lead to
    # success (because the other data prep dag might be ready)
    'retry_delay': timedelta(minutes=90),
    'max_active_runs': 1,
    'execution_timeout': timedelta(hours=10),
    'executor_config': {
        'KubernetesExecutor': {
            'request_cpu': "200m",
            'limit_cpu': "1000m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi",
        },
    },
}

experimental_config = config.get('order-forecast-experimental', {'models': {}})

countries_inactive = Variable.get('order-forecast-countries-inactive', default_var=[], deserialize_json=True)
countries_disabled = Variable.get('order-forecast-countries-disabled', default_var=[], deserialize_json=True)

# gcs and s3 data lake bucket
data_lake_bucket = Variable.get('data-science-data-lake-bucket', default_var='staging-eu-data-science-data-lake')
feature_store_path = 'feature_store/forecast_dataset'
model_output_bucket = Variable.get('data-science-model-outputs-bucket', default_var='staging-eu-data-science-model-outputs')
model_output_path = 'order_forecast_output'


skip_forecast_exists_check = Variable.get('order-forecast-skip-forecast-exists-check',
                                          default_var='false').lower() == 'true'

dag = DAG(
    dag_id=f'forecast-orders-experimental-models-v{version}',
    description=f'Run forecast preprocessing ETLs on BigQuery and models via kubernetes operator.',
    schedule_interval=schedule,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath=f'{dags_folder}/forecast/bigquery',
    catchup=False,
    tags=[default_args['owner']],
)

residuals_operator = create_bq_table_operator(dag,
                                              query_name='residuals',
                                              task_id='create_residuals_table',
                                              maximum_gb_billed=1000)

for region in config['regions']:
    start_region_operator = DummyOperator(dag=dag, task_id=f'start_region_{region}')
    countries = get_countries(region, countries_inactive, countries_disabled)

    residuals_operator >> start_region_operator

    for country_code in countries:
        start_country_operator = DummyOperator(dag=dag, task_id=f'start_{country_code}')

        start_region_operator >> start_country_operator

        for model_params in experimental_config['models']:
            model_name = model_params['model_name']
            version = model_params.get('version', 'latest')

            # production should be skipped in this DAG
            if model_name == 'production':
                continue

            forecast_exists_check_operator = create_forecast_exists_check_operator(
                parent_dag=dag,
                country_code=country_code,
                model_name=model_name,
                model_output_bucket=model_output_bucket,
                model_output_path=model_output_path,
                skip_check=skip_forecast_exists_check
            )

            order_forecast_operator = create_order_forecast_operator(
                parent_dag=dag,
                country_code=country_code,
                model_parameters=model_params,
                version=version,
                data_lake_bucket=data_lake_bucket,
                feature_store_path=feature_store_path,
                model_output_bucket=model_output_bucket,
                model_output_path=model_output_path,
                skip_sentry=True,
            )

            export_to_gcs_operator = create_s3_to_gcs_operator(
                parent_dag=dag,
                country_code=country_code,
                model_output_bucket=model_output_bucket,
                model_output_path=model_output_path,
                model_name=model_name
            )

            load_to_bq_operator = create_gcs_to_bq_operator(
                parent_dag=dag,
                country_code=country_code,
                model_output_bucket=model_output_bucket,
                model_output_path=model_output_path,
                model_name=model_name
            )

            (start_country_operator
             >> forecast_exists_check_operator
             >> order_forecast_operator
             >> export_to_gcs_operator
             >> load_to_bq_operator)
