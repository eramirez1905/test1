from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from configuration import config

from forecast.operators import create_bq_table_operator, \
    create_up_to_date_check_operator, create_bqq_to_gcs_fs_operator, create_gcs_to_s3_operator, \
    create_bq_fs_validation_operator, \
    create_order_forecast_imputation_operator, create_order_forecast_operator, \
    create_forecast_exists_check_operator, create_s3_to_gcs_operator, \
    create_gcs_to_bq_operator, create_sqs_publish_operator


version = 3
dags_folder = configuration.get('core', 'dags_folder')
schedule = '0 10,14 * * *'

default_args = {
    'owner': 'data-science',
    'start_date': datetime(2019, 2, 17),
    'retries': 2,
    'retry_delay': timedelta(minutes=20),
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

dag_config = config['dmart-order-forecast']
regions = dag_config['regions']

data_lake_bucket = Variable.get('data-science-data-lake-bucket', default_var='production-eu-data-science-data-lake')
feature_store_path = 'feature_store/dmart_order_forecast'

model_output_bucket = Variable.get('data-science-model-outputs-bucket', default_var='production-eu-data-science-model-outputs')
model_output_path = 'dmart_order_forecast'

version_forecast = Variable.get('dmart-order-forecast-forecast-version', default_var='latest')
version_imputation = Variable.get('dmart-order-forecast-imputation-version', default_var='latest')

default_model_parameters = {"model_name": "production", "forecast_horizon": 50}
model_parameters = Variable.get('dmart-order-forecast-production-parameters', default_var=default_model_parameters, deserialize_json=True)

# dag
dag = DAG(
    dag_id=f'dmart-order-forecast-v{version}',
    description=f'Run dmart order forecast preprocessing ETLs on BigQuery and models via kubernetes operator.',
    schedule_interval=schedule,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath=f'{dags_folder}/forecast/dmart_orders/sql',
    catchup=False,
    tags=[default_args['owner']],
)

zones_operator = create_bq_table_operator(dag, query_name='zones')
vendor_orders_timeseries_operator = create_bq_table_operator(dag, query_name='vendor_orders_timeseries')
orders_timeseries_operator = create_bq_table_operator(dag, query_name='orders_timeseries')
timeseries_operator = create_bq_table_operator(dag, query_name='timeseries')

end_feature_extraction_operator = DummyOperator(dag=dag, task_id='end_feature_extraction')

(zones_operator
 >> vendor_orders_timeseries_operator
 >> orders_timeseries_operator
 >> timeseries_operator)

timeseries_operator >> end_feature_extraction_operator

# a dataset query creates a global dataset
dataset_queries = ['dataset_zone_hour',
                   'dataset_holidays',
                   'dataset_holiday_periods']

# a feature store query MUST consist of 2 queries, both parametrized in country_code:
# 2) a validation query, which is executed before persisting the results in gcs/s3, and
# 3) a feature store query, which is used to persist the results to gcs/s3.
feature_store_queries = ['dataset_zone_hour',
                         'dataset_holidays',
                         'dataset_holiday_periods']

end_dataset_extraction_operator = DummyOperator(dag=dag, task_id='end_dataset_extraction')

for dataset_query in dataset_queries:
    dataset_operator = create_bq_table_operator(dag, f'datasets/{dataset_query}')

    (end_feature_extraction_operator
     >> dataset_operator
     >> end_dataset_extraction_operator)

for region in regions:
    start_region_export_operator = DummyOperator(dag=dag, task_id=f'start_export_{region}')

    end_dataset_extraction_operator >> start_region_export_operator

    countries = regions[region]['countries']
    sqs_queue_url = regions[region]['sqs']

    for country_code in countries:
        bq_up_to_date_check_operator = create_up_to_date_check_operator(dag, country_code)

        start_region_export_operator >> bq_up_to_date_check_operator

        order_forecast_imputation_operator = create_order_forecast_imputation_operator(
            parent_dag=dag,
            country_code=country_code,
            version=version_imputation,
            data_lake_bucket=data_lake_bucket,
            feature_store_path=feature_store_path
        )

        for feature_store_query in feature_store_queries:
            bqq_to_gcs_operator = create_bqq_to_gcs_fs_operator(
                parent_dag=dag,
                query_name=feature_store_query,
                country_code=country_code,
                data_lake_bucket=data_lake_bucket,
                feature_store_path=feature_store_path
            )

            gcs_to_s3_operator = create_gcs_to_s3_operator(
                parent_dag=dag,
                query_name=feature_store_query,
                country_code=country_code,
                data_lake_bucket=data_lake_bucket,
                feature_store_path=feature_store_path
            )

            bq_check_operator = create_bq_fs_validation_operator(
                parent_dag=dag,
                query_name=feature_store_query,
                country_code=country_code
            )

            (bq_up_to_date_check_operator
             >> bq_check_operator
             >> bqq_to_gcs_operator
             >> gcs_to_s3_operator
             >> order_forecast_imputation_operator
             )

        forecast_exists_check_operator = create_forecast_exists_check_operator(
            parent_dag=dag,
            country_code=country_code,
            model_name='production',
            model_output_bucket=model_output_bucket,
            model_output_path=model_output_path,
            skip_check=False
        )

        order_forecast_operator = create_order_forecast_operator(
            parent_dag=dag,
            country_code=country_code,
            model_parameters=model_parameters,
            version=version_forecast,
            data_lake_bucket=data_lake_bucket,
            feature_store_path=feature_store_path,
            model_output_bucket=model_output_bucket,
            model_output_path=model_output_path
        )

        sqs_publish_operator = create_sqs_publish_operator(
            parent_dag=dag,
            country_code=country_code,
            model_output_bucket=model_output_bucket,
            model_output_path=model_output_path,
            queue_url=sqs_queue_url,
        )

        # TODO: add latest only operator in between the following steps, currently does not work
        # as it skips the operator if the next dag runs execution_date is before actual time the task is run.
        (order_forecast_operator
         >> sqs_publish_operator)

        export_to_gcs_operator = create_s3_to_gcs_operator(
            parent_dag=dag,
            country_code=country_code,
            model_output_bucket=model_output_bucket,
            model_output_path=model_output_path,
            model_name='production',
        )

        load_to_bq_operator = create_gcs_to_bq_operator(
            parent_dag=dag,
            country_code=country_code,
            model_output_bucket=model_output_bucket,
            model_output_path=model_output_path,
            model_name='production',
        )

        (order_forecast_imputation_operator
         >> forecast_exists_check_operator
         >> order_forecast_operator
         >> export_to_gcs_operator
         >> load_to_bq_operator)
