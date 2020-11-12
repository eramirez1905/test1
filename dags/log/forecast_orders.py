from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from configuration import config
from forecast.operators import create_bq_table_operator, create_bq_table_validation_operator, \
    create_up_to_date_check_operator, create_bqq_to_gcs_fs_operator, create_gcs_to_s3_operator, \
    create_bq_fs_validation_operator, \
    create_order_forecast_imputation_operator, create_order_forecast_operator, \
    create_forecast_exists_check_operator, create_s3_to_gcs_operator, create_sqs_publish_operator, \
    create_gcs_to_bq_operator, create_model_weights_update_check_operator
from forecast.utils import get_countries

version = 6
dags_folder = configuration.get('core', 'dags_folder')

# cl runs at 00:30, 05:30, 08:30, 20:30 UTC and takes around 2-3.5h
schedule = '0 0,8,16 * * *'

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

regions = Variable.get('order-forecast-regions', default_var=config['regions'], deserialize_json=True)
countries_inactive = Variable.get('order-forecast-countries-inactive', default_var=[], deserialize_json=True)
countries_disabled = Variable.get('order-forecast-countries-disabled', default_var=[], deserialize_json=True)

# gcs and s3 data lake bucket
data_lake_bucket = Variable.get('data-science-data-lake-bucket',
                                default_var='production-eu-data-science-data-lake')
feature_store_path = 'feature_store/forecast_dataset'

model_output_bucket = Variable.get('data-science-model-outputs-bucket',
                                   default_var='production-eu-data-science-model-outputs')
model_output_path = 'order_forecast_output'

version_forecast = Variable.get('order-forecast-forecast-version', default_var='latest')
version_imputation = Variable.get('order-forecast-imputation-version', default_var='latest')

model_parameters = Variable.get('order-forecast-production-parameters',
                                default_var={"model_name": "production", "forecast_horizon": 50},
                                deserialize_json=True)

skip_forecast_exists_check = Variable.get('order-forecast-skip-forecast-exists-check',
                                          default_var='false').lower() == 'true'

k4_hack = False

dag = DAG(
    dag_id=f'forecast-orders-v{version}',
    description=f'Run forecast preprocessing ETLs on BigQuery and models via kubernetes operator.',
    schedule_interval=schedule,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath=f'{dags_folder}/forecast/bigquery',
    catchup=False,
    tags=[default_args['owner']],
)

zones_operator = create_bq_table_operator(dag,
                                          query_name='zones',
                                          params={
                                              'countries_inactive': countries_inactive,
                                              'dataset_raw': config.get('bigquery').get('dataset').get('raw'),
                                              'dataset_cl': 'cl'
                                          })

orders_timeseries_operator = create_bq_table_operator(dag,
                                                      query_name='orders_timeseries',
                                                      params={'countries_inactive': countries_inactive})
orders_timeseries_validation_operator = create_bq_table_validation_operator(dag, 'orders_timeseries')

events_operator = create_bq_table_operator(dag, 'events')
events_to_zones_operator = create_bq_table_operator(dag, 'events_to_zones')
events_grid_operator = create_bq_table_operator(dag, 'events_grid')

events_timeseries_operator = create_bq_table_operator(dag, 'events_timeseries')
events_timeseries_validation_operator = create_bq_table_validation_operator(dag, 'events_timeseries')

adjustments_timeseries_operator = create_bq_table_operator(dag, 'adjustments_timeseries')
adjustments_timeseries_validation_operator = create_bq_table_validation_operator(dag, 'adjustments_timeseries')

# lost orders:
events_merged_aligned_operator = create_bq_table_operator(dag, 'lost_orders/events_merged_aligned')
events_orders_history_all_operator = create_bq_table_operator(dag, 'lost_orders/events_orders_history_all')
orders_lost_extrapolated_operator = create_bq_table_operator(dag, 'lost_orders/orders_lost_extrapolated')

orders_lost_timeseries_operator = create_bq_table_operator(dag, 'orders_lost_timeseries')
orders_lost_timeseries_validation_operator = create_bq_table_validation_operator(dag, 'orders_lost_timeseries')

timeseries_operator = create_bq_table_operator(dag, 'timeseries')

end_feature_extraction_operator = DummyOperator(dag=dag, task_id='end_feature_extraction')

(zones_operator
 >> orders_timeseries_operator
 >> orders_timeseries_validation_operator
 >> timeseries_operator)

(zones_operator
 >> events_operator
 >> events_to_zones_operator
 >> events_grid_operator
 >> events_timeseries_operator
 >> events_timeseries_validation_operator
 >> timeseries_operator)

(zones_operator
 >> adjustments_timeseries_operator
 >> adjustments_timeseries_validation_operator
 >> timeseries_operator)

events_to_zones_operator >> events_merged_aligned_operator

(events_merged_aligned_operator
 >> events_orders_history_all_operator
 >> orders_lost_extrapolated_operator)

(orders_lost_extrapolated_operator
 >> orders_lost_timeseries_operator
 >> orders_lost_timeseries_validation_operator
 >> timeseries_operator)

timeseries_operator >> end_feature_extraction_operator

# a dataset query creates a global dataset
dataset_queries = ['dataset_zone_hour',
                   'dataset_city_day',
                   'dataset_holidays',
                   'dataset_holiday_periods']

# a feature store query MUST consist of 2 queries, both parametrized in country_code:
# 2) a validation query, which is executed before persisting the results in gcs/s3, and
# 3) a feature store query, which is used to persist the results to gcs/s3.
feature_store_queries = ['dataset_zone_hour',
                         'dataset_city_day',
                         'dataset_holidays',
                         'dataset_holiday_periods',
                         'dataset_ts_model_weights']

end_dataset_extraction_operator = DummyOperator(dag=dag, task_id='end_dataset_extraction')

# TODO: hack for k3->k4 projection - remove once done
if k4_hack:
    dataset_zone_hour_k4_operator = create_bq_table_operator(dag, f'datasets/dataset_zone_hour_k4')

for dataset_query in dataset_queries:
    dataset_operator = create_bq_table_operator(dag, f'datasets/{dataset_query}')

    # TODO: hack for k3->k4 projection - remove once done
    if k4_hack and dataset_query == 'dataset_zone_hour':
        (end_feature_extraction_operator
         >> dataset_zone_hour_k4_operator
         >> dataset_operator
         )

    (end_feature_extraction_operator
     >> dataset_operator
     >> end_dataset_extraction_operator)

# check if the ts_model_weights table needs an update and update if necessary
ts_model_weights_update_necessary_operator = create_model_weights_update_check_operator(dag)
# set maximum_gb_billed to 2500, query is quite expensive but runs only once per month
ts_model_weights_operator = create_bq_table_operator(dag,
                                                     'datasets/dataset_ts_model_weights',
                                                     maximum_gb_billed=2500)

(end_feature_extraction_operator
 >> ts_model_weights_update_necessary_operator
 >> ts_model_weights_operator)

for region in config['regions']:
    start_region_export_operator = DummyOperator(dag=dag, task_id=f'start_export_{region}')

    end_dataset_extraction_operator >> start_region_export_operator

    countries = get_countries(region, countries_inactive, countries_disabled)

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
            skip_check=skip_forecast_exists_check
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
