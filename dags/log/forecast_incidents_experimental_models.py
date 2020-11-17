from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.models import Variable

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.s3_to_gcs_operator import S3ToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from forecast.operators import create_incident_forecast_operator
from forecast.utils import get_incident_forecast_output_schema
from configuration import config


version = 1
dags_folder = configuration.get('core', 'dags_folder')
schedule = '0 22 * * *'

data_lake_bucket = Variable.get('data-science-data-lake-bucket', default_var='staging-eu-data-science-data-lake')
model_output_bucket = Variable.get('data-science-model-outputs-bucket', default_var='staging-eu-data-science-model-outputs')
feature_store_path = 'feature_store/incidents'
model_output_path = 'incident_forecast_output'
model_input_filename = 'dataset_incidents'
model_output_filename = 'forecasts'
run_date = '{{ next_ds }}'
forecast_version = Variable.get('incident-forecast-version', default_var='latest')
start_time = datetime.now().strftime("%Y%m%d%H%M%S")
ui_color = '#d0d5d6'

experimental_config = config.get('incident-forecast-experimental', {'models': {}})

# DAG default arguments
default_args = {
    'owner': 'data-science',
    'start_date': datetime(2019, 10, 27),
    'retries': 2,
    'retry_delay': timedelta(minutes=90),
    'max_active_runs': 1,
    'execution_timeout': timedelta(hours=10)
}

dag = DAG(
    dag_id=f'forecast-incidents-experimental-models-v{version}',
    description='Running experimental forecast models.',
    schedule_interval=schedule,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath=f'{dags_folder}/forecast/incidents/sql',
    catchup=False,
    tags=[default_args['owner']],
)

start_operator = DummyOperator(
    dag=dag,
    task_id='start'
)
start_operator.ui_color = ui_color

for country_iso in config['contact_center']['countries']:

    start_country_operator = DummyOperator(
        dag=dag,
        task_id=f'start_{country_iso}'
    )
    start_operator >> start_country_operator

    for model_params in experimental_config['models']:
        model_name = model_params['model_name']
        forecast_version = model_params.get('version', forecast_version)

        forecast_parameters = dict(country_iso=country_iso,
                                   model_name=model_name,
                                   version=forecast_version,
                                   input_bucket=data_lake_bucket,
                                   input_folder=f'{feature_store_path}/{{{{ds}}}}/{country_iso.lower()}/',
                                   output_bucket=model_output_bucket,
                                   output_folder=f'{model_output_path}/{{{{ds}}}}/{country_iso.lower()}/{model_name}/',
                                   input_filename=model_input_filename,
                                   output_filename=model_output_filename,
                                   run_date=run_date,
                                   task_id=f'run_forecast_{model_name}_{country_iso.lower()}',
                                   request_id=f'run_forecast_{model_name}_{country_iso.lower()}_{str(start_time)}'
                                   )

        # running the forecast model
        incident_forecast_operator = create_incident_forecast_operator(
            parent_dag=dag,
            task_id=forecast_parameters['task_id'],
            parameters=forecast_parameters,
            cmds='python3',
            entrypoint='runner/run_incident_forecast_model.py',
            on_success_callback=None,
            on_failure_callback=None,
            skip_sentry=True
        )

        # Pushing the forecast results from S3 to GCS
        model_output_filename_tz = model_output_filename + '_tz'
        prefix = f'{forecast_parameters["output_folder"]}{model_output_filename_tz}'
        gcs_destination = f'gs://{model_output_bucket}/'

        forecast_to_gcs_operator = S3ToGoogleCloudStorageOperator(
            dag=dag,
            task_id=f'forecast_output_{model_name}_{country_iso.lower()}_to_gcs',
            bucket=model_output_bucket,
            prefix=prefix,
            dest_gcs_conn_id='google_cloud_default',
            dest_gcs=gcs_destination,
            replace=True,
            execution_timeout=timedelta(minutes=20)
        )
        forecast_to_gcs_operator.ui_color = ui_color

        # Pushing the forecasts into BQ
        # into a single table for all countries
        forecast_to_bq_operator = GoogleCloudStorageToBigQueryOperator(
            dag=dag,
            task_id=f'forecast_output_{model_name}_{country_iso.lower()}_to_bq',
            bucket=model_output_bucket,
            source_objects=[prefix + '.csv.gz'],
            schema_fields=get_incident_forecast_output_schema(),
            destination_project_dataset_table='incident_forecast.forecasts_country_iso_experimental',
            autodetect=False,
            skip_leading_rows=1,
            compression='GZIP',
            field_delimiter=';',
            source_format='CSV',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND',
            ignore_unknown_values=True,
            execution_timeout=timedelta(minutes=20)
        )
        forecast_to_bq_operator.ui_color = ui_color

        (start_country_operator
         >> incident_forecast_operator
         >> forecast_to_gcs_operator
         >> forecast_to_bq_operator
         )
