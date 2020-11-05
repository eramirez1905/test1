from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator
from airflow.contrib.operators.s3_to_gcs_operator import S3ToGoogleCloudStorageOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from configuration import config
from datahub.common import alerts
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from datahub.operators.json_s3_operator import JSONToS3Operator
from forecast.operators import create_incident_forecast_operator, create_sqs_publish_operator
from forecast.utils import get_contact_center_environments
from forecast.utils import get_incident_forecast_output_schema

version = 5
dags_folder = configuration.get('core', 'dags_folder')
schedule = '0 16 * * *'

data_lake_bucket = Variable.get('data-science-data-lake-bucket', default_var='staging-eu-data-science-data-lake')
model_output_bucket = Variable.get('data-science-model-outputs-bucket', default_var='staging-eu-data-science-model-outputs')
feature_store_path = 'feature_store/incidents'
model_output_path = 'incident_forecast_output'
model_input_filename = 'dataset_incidents'
model_output_filename = 'forecasts'
model_name = 'production'
run_date = '{{ next_ds }}'
forecast_version = Variable.get('incident-forecast-version', default_var='latest')
start_time = datetime.now().strftime("%Y%m%d%H%M%S")
ui_color = '#d0d5d6'

# DAG default arguments
default_args = {
    'owner': 'data-science',
    'start_date': datetime(2019, 10, 27),
    'retries': 2,
    'retry_delay': timedelta(minutes=20),
    'max_active_runs': 1,
    'execution_timeout': timedelta(hours=10)
}

dag = DAG(
    dag_id=f'forecast-incidents-v{version}',
    description='Forecasting Contact Center Incidents per half-hour.',
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

end_operator = DummyOperator(
    dag=dag,
    task_id='end',
    on_success_callback=alerts.setup_callback()
)
end_operator.ui_color = '#d0d5d6'

validation_done_operator = DummyOperator(
    dag=dag,
    task_id='input_data_validation_done',
    on_success_callback=alerts.setup_callback()
)
validation_done_operator.ui_color = '#d0d5d6'

pre_processing_done_operator = DummyOperator(
    dag=dag,
    task_id='done_pre_processing_start_forecast'
)
pre_processing_done_operator.ui_color = ui_color

# Running checks on the table containing incidents data, imported daily from Pandora DWH
incidents_table_validation_operator = BigQueryCheckOperator(
    dag=dag,
    task_id=f'validate_pandora_traffic_table',
    sql=f'check_pandora_traffic.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default',
)
incidents_table_validation_operator.ui_color = ui_color

# Running checks on the table mapping incidents to zones, imported daily from Pandora DWH
mapping_table_validation_operator = BigQueryCheckOperator(
    dag=dag,
    task_id=f'validate_incidents_to_zones_map_table',
    sql=f'check_pandora_incidents_to_zone_map.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default',
)
mapping_table_validation_operator.ui_color = ui_color

# Read the data mapping table sent from Pandora DWH
incidents_to_zones_map_bq_operator = BigQueryOperator(
    dag=dag,
    task_id='incidents_to_zone_map',
    sql='rooster_incidents_to_zone_map.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default'
)
incidents_to_zones_map_bq_operator.ui_color = ui_color

# Store map as csv file in GCS/S3
destination = f'gs://{data_lake_bucket}/{feature_store_path}/' \
              f'{{{{ds}}}}/incidents-to-zones-map/incidents_to_zones_map.csv.gz'
incidents_to_zones_map_to_gcs_operator = BigQueryToCloudStorageOperator(
    dag=dag,
    task_id='incidents_to_zones_map_to_gcs',
    source_project_dataset_table=f'incident_forecast.rooster_incidents_to_zones_map',
    destination_cloud_storage_uris=[destination],
    compression='GZIP',
    export_format='CSV',
    field_delimiter=';',
    print_header=True,
    bigquery_conn_id='bigquery_default'
)
incidents_to_zones_map_to_gcs_operator.ui_color = ui_color

prefix = f'{feature_store_path}/{{{{ds}}}}/incidents-to-zones-map/incidents_to_zones_map'
incidents_to_zones_map_to_s3_operator = GoogleCloudStorageToS3Operator(
    dag=dag,
    task_id='incidents_to_zones_map_to_s3',
    google_cloud_storage_conn_id='google_cloud_default',
    dest_aws_conn_id='aws_default',
    bucket=data_lake_bucket,
    prefix=prefix,
    dest_s3_key=f's3://{data_lake_bucket}/',
    replace=True
)
incidents_to_zones_map_to_s3_operator.ui_color = ui_color

# Build an OD Orders Dataset
# Historical orders for past values, forecasts for future values
od_orders_bq_operator = BigQueryOperator(
    dag=dag,
    task_id='od_orders_to_country_iso',
    sql='od_orders_country_iso.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default'
)
od_orders_bq_operator.ui_color = ui_color

# Filter the data provided by Pandora BI
# and change the format from wide to long
filter_incidents_data_bq_operator = BigQueryOperator(
    dag=dag,
    task_id='filter_incidents_data',
    sql='filtered_cc_incidents.sql',
    params={'forecast_countries': str(config['contact_center']['countries']).replace('[', '').replace(']', ''),
            'incident_types': str(config['contact_center']['incident_types']).replace('[', '').replace(']', '')},
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default'
)
filter_incidents_data_bq_operator.ui_color = ui_color

# Build a the forecasting dataset in the country_iso level
# Merge the long cc_incidents table with the od_orders table
training_data_bq_operator = BigQueryOperator(
    dag=dag,
    task_id='build_training_dataset',
    sql=f'training_data_country_iso.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default'
)
training_data_bq_operator.ui_color = ui_color

# Creating a table with historical incidents per zone
histories_table_bq_operator = BigQueryOperator(
    dag=dag,
    task_id='build_histories_table',
    sql='incident_history_zones.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default'
)
histories_table_bq_operator.ui_color = ui_color

# Extract per country, brand, incident type and hour
# the percentage of each used language
language_percentage_bq_operator = BigQueryOperator(
    dag=dag,
    task_id='extract_percentage_per_language_and_hour',
    sql='country_incident_language_percentage.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default'
)
language_percentage_bq_operator.ui_color = ui_color

# Running checks on the language percentage table
language_percentage_validation_operator = BigQueryCheckOperator(
    dag=dag,
    task_id=f'validate_percentage_per_language_and_hour',
    sql=f'check_country_incident_language_percentage.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default',
)
language_percentage_validation_operator.ui_color = ui_color

# Store language percentages as csv file in GCS/S3
destination = f'gs://{data_lake_bucket}/{feature_store_path}/' \
              f'{{{{ds}}}}/language-percentages/language_percentages.csv.gz'
language_percentages_to_gcs_operator = BigQueryToCloudStorageOperator(
    dag=dag,
    task_id='language_percentages_to_gcs',
    source_project_dataset_table=f'incident_forecast.country_incident_language_percentage',
    destination_cloud_storage_uris=[destination],
    compression='GZIP',
    export_format='CSV',
    field_delimiter=';',
    print_header=True,
    bigquery_conn_id='bigquery_default'
)
language_percentages_to_gcs_operator.ui_color = ui_color

prefix = f'{feature_store_path}/{{{{ds}}}}/language-percentages/language_percentages'
language_percentages_to_s3_operator = GoogleCloudStorageToS3Operator(
    dag=dag,
    task_id='language_percentages_to_s3',
    google_cloud_storage_conn_id='google_cloud_default',
    dest_aws_conn_id='aws_default',
    bucket=data_lake_bucket,
    prefix=prefix,
    dest_s3_key=f's3://{data_lake_bucket}/',
    replace=True
)
language_percentages_to_s3_operator.ui_color = ui_color

(start_operator
 >> [incidents_table_validation_operator, mapping_table_validation_operator]
 >> validation_done_operator
 >> [od_orders_bq_operator, filter_incidents_data_bq_operator, incidents_to_zones_map_bq_operator]
 >> training_data_bq_operator
 )

(incidents_to_zones_map_bq_operator
 >> incidents_to_zones_map_to_gcs_operator
 >> incidents_to_zones_map_to_s3_operator
 >> pre_processing_done_operator
 )

(training_data_bq_operator
 >> [histories_table_bq_operator, language_percentage_bq_operator]
 >> pre_processing_done_operator
 )

(language_percentage_bq_operator
 >> language_percentage_validation_operator
 >> language_percentages_to_gcs_operator
 >> language_percentages_to_s3_operator
 >> pre_processing_done_operator)

# Aggregate the forecasts to the language level
# and stack all forecasts on top into one table
language_split_bq_operator = BigQueryOperator(
    dag=dag,
    task_id='language-split',
    sql='forecasts_country_iso_language.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default'
)
language_split_bq_operator.ui_color = ui_color

for country_iso in config['contact_center']['countries']:

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
                               task_id=f'run_forecast_production_{country_iso.lower()}',
                               request_id=f'run_forecast_production_{country_iso.lower()}_{str(start_time)}'
                               )

    # Splitting up the different countries and aggregating in each country across all languages
    split_countries_bq_operator = BigQueryOperator(
        dag=dag,
        task_id=f'separate_country_iso_{country_iso.lower()}',
        sql='split_country_isos.sql',
        params={'country_iso': country_iso,
                'country_iso_low': country_iso.lower()},
        use_legacy_sql=False,
        bigquery_conn_id='bigquery_default'
    )
    split_countries_bq_operator.ui_color = ui_color

    # Loading the data into GCS as compressed csv
    destination = f'gs://{data_lake_bucket}/{feature_store_path}/' \
                  f'{{{{ds}}}}/{country_iso.lower()}/{model_input_filename}.csv.gz'

    bq_country_to_gcs_operator = BigQueryToCloudStorageOperator(
        dag=dag,
        task_id=f'training_data_{country_iso.lower()}_to_gcs',
        source_project_dataset_table=f'incident_forecast.training_data_country_iso_{country_iso.lower()}',
        destination_cloud_storage_uris=[destination],
        compression='GZIP',
        export_format='CSV',
        field_delimiter=';',
        print_header=True,
        bigquery_conn_id='bigquery_default'
    )
    bq_country_to_gcs_operator.ui_color = ui_color

    # Loading the data from GCS to S3
    prefix = f'{feature_store_path}/{{{{ds}}}}/{country_iso.lower()}/{model_input_filename}'

    gcs_country_to_s3_operator = GoogleCloudStorageToS3Operator(
        dag=dag,
        task_id=f'training_data_{country_iso.lower()}_to_s3',
        google_cloud_storage_conn_id='google_cloud_default',
        dest_aws_conn_id='aws_default',
        bucket=data_lake_bucket,
        prefix=prefix,
        dest_s3_key=f's3://{data_lake_bucket}/',
        replace=True
    )
    gcs_country_to_s3_operator.ui_color = ui_color

    # Running the forecasting per incident type
    # and making the relevant timezone adjustments
    # The results are stored in an S3 bucket
    incident_forecast_operator = create_incident_forecast_operator(
        parent_dag=dag,
        task_id=f'run_forecast_model_{country_iso.lower()}',
        parameters=forecast_parameters,
        cmds='python3',
        entrypoint='runner/run_incident_forecast_model.py'
    )

    # Pushing the forecast results from S3 to GCS
    model_output_filename_tz = model_output_filename + '_tz'
    prefix = f'{forecast_parameters["output_folder"]}{model_output_filename_tz}'
    gcs_destination = f'gs://{model_output_bucket}/'

    forecast_to_gcs_operator = S3ToGoogleCloudStorageOperator(
        dag=dag,
        task_id=f'forecast_output_{country_iso.lower()}_to_gcs',
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
        task_id=f'forecast_output_{country_iso.lower()}_to_bq',
        bucket=model_output_bucket,
        source_objects=[prefix + '.csv.gz'],
        schema_fields=get_incident_forecast_output_schema(),
        destination_project_dataset_table='incident_forecast.forecasts_country_iso',
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

    (pre_processing_done_operator
     >> split_countries_bq_operator
     >> bq_country_to_gcs_operator
     >> gcs_country_to_s3_operator
     >> incident_forecast_operator
     >> forecast_to_gcs_operator
     >> forecast_to_bq_operator
     >> language_split_bq_operator
     )

# Mapping Forecasts to Rooster Zones and aggregating them
zone_aggregation_bq_operator = BigQueryOperator(
    dag=dag,
    task_id='forecasts_to_zones',
    sql='incident_forecast_zones.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default'
)
zone_aggregation_bq_operator.ui_color = ui_color

language_split_bq_operator >> zone_aggregation_bq_operator

# Splitting the Forecasts and Histories across Regions and Environments
for region in config['contact_center']['regions']:
    start_region_export_operator = DummyOperator(dag=dag, task_id=f'start_export_{region}')
    start_region_export_operator.ui_color = '#d0d5d6'
    zone_aggregation_bq_operator >> start_region_export_operator

    for environment in get_contact_center_environments(region):
        env_underscore = environment.replace('-', '_')

        manifest_parameters = dict(output_bucket=model_output_bucket,
                                   version=forecast_version,
                                   country_code=environment,
                                   output_folder=f'{model_output_path}/{{{{ds}}}}/{environment}/{model_name}/',
                                   request_id=f'publish_forecast_{env_underscore}_{str(start_time)}'
                                   )

        environment_forecast_to_bq_operator = BigQueryOperator(
            dag=dag,
            task_id=f'forecast_{env_underscore}_to_bq',
            sql='forecast_to_environment.sql',
            params={'environment': environment,
                    'environment_underscore': env_underscore},
            use_legacy_sql=False,
            bigquery_conn_id='bigquery_default'
        )
        environment_forecast_to_bq_operator.ui_color = ui_color

        destination = f'gs://{model_output_bucket}/{model_output_path}/' \
                      f'{{{{ds}}}}/{environment}/{model_name}/forecasts.csv.gz'
        environment_forecast_to_gcs_operator = BigQueryToCloudStorageOperator(
            dag=dag,
            task_id=f'forecast_{env_underscore}_to_gcs',
            source_project_dataset_table=f'incident_forecast.forecasts_environment_{env_underscore}',
            destination_cloud_storage_uris=[destination],
            compression='GZIP',
            export_format='CSV',
            field_delimiter=';',
            print_header=True,
            bigquery_conn_id='bigquery_default'
        )
        environment_forecast_to_gcs_operator.ui_color = ui_color

        prefix = f'{model_output_path}/{{{{ds}}}}/{environment}/{model_name}/forecasts'
        environment_forecast_to_s3_operator = GoogleCloudStorageToS3Operator(
            dag=dag,
            task_id=f'forecast_{environment}_to_s3',
            google_cloud_storage_conn_id='google_cloud_default',
            dest_aws_conn_id='aws_default',
            bucket=model_output_bucket,
            prefix=prefix,
            dest_s3_key=f's3://{model_output_bucket}/',
            replace=True
        )
        environment_forecast_to_s3_operator.ui_color = ui_color

        environment_history_to_bq_operator = BigQueryOperator(
            dag=dag,
            task_id=f'history_{environment}_to_bq',
            sql='history_to_environment.sql',
            params={'environment': environment,
                    'environment_underscore': env_underscore},
            use_legacy_sql=False,
            bigquery_conn_id='bigquery_default'
        )
        environment_history_to_bq_operator.ui_color = ui_color

        destination = f'gs://{model_output_bucket}/{model_output_path}/' \
                      f'{{{{ds}}}}/{environment}/{model_name}/histories.csv.gz'
        environment_history_to_gcs_operator = BigQueryToCloudStorageOperator(
            dag=dag,
            task_id=f'history_{environment}_to_gcs',
            source_project_dataset_table=f'incident_forecast.incidents_history_environment_{env_underscore}',
            destination_cloud_storage_uris=[destination],
            compression='GZIP',
            export_format='CSV',
            field_delimiter=';',
            print_header=True,
            bigquery_conn_id='bigquery_default'
        )
        environment_history_to_gcs_operator.ui_color = ui_color

        prefix = f'{model_output_path}/{{{{ds}}}}/{environment}/{model_name}/histories'
        environment_history_to_s3_operator = GoogleCloudStorageToS3Operator(
            dag=dag,
            task_id=f'history_{environment}_to_s3',
            google_cloud_storage_conn_id='google_cloud_default',
            dest_aws_conn_id='aws_default',
            bucket=model_output_bucket,
            prefix=prefix,
            dest_s3_key=f's3://{model_output_bucket}/',
            replace=True
        )
        environment_history_to_s3_operator.ui_color = ui_color

        # Creating and storing the Manifest.json file
        key = f'{model_output_path}/{{{{ds}}}}/{environment}/{model_name}/manifest.json'
        create_manifest_operator = JSONToS3Operator(
            dictionary=manifest_parameters,
            key=key,
            bucket=model_output_bucket,
            s3_conn_id='aws_default',
            dag=dag,
            task_id=f'manifest_{environment}_to_s3'
        )
        create_manifest_operator.ui_color = ui_color

        # Writing an SQS Message to indicate location of new forecasts
        sqs_publish_operator = create_sqs_publish_operator(
            parent_dag=dag,
            country_code=environment,
            model_output_bucket=model_output_bucket,
            model_output_path=model_output_path,
            model_name=model_name,
            date='{{ds}}'
        )
        sqs_publish_operator.ui_color = ui_color

        (start_region_export_operator
         >> [environment_forecast_to_bq_operator, environment_history_to_bq_operator])

        (environment_forecast_to_bq_operator
         >> environment_forecast_to_gcs_operator
         >> environment_forecast_to_s3_operator
         )

        (environment_history_to_bq_operator
         >> environment_history_to_gcs_operator
         >> environment_history_to_s3_operator
         )

        ([environment_history_to_s3_operator, environment_forecast_to_s3_operator]
         >> create_manifest_operator
         >> sqs_publish_operator
         >> end_operator)
