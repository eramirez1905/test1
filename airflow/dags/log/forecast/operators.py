import json
from datetime import timedelta, datetime

from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator
from airflow.contrib.operators.s3_to_gcs_operator import S3ToGoogleCloudStorageOperator
from airflow.models import Variable
from airflow.operators.python_operator import ShortCircuitOperator

from configuration import config
from datahub.common import alerts
from datahub.operators.aws_sqs_publish_operator import SQSPublishOperator
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from datahub.operators.bigquery_export_query_to_gcs_operator import BigQueryExportQueryToGCSOperator
from forecast.utils import get_yesterday, get_run_date, country_is_up_to_date, model_weights_need_update, \
    forecast_does_not_exist, get_forecast_output_schema, get_region, get_queue_url, get_aws_conn_id
from operators.incident_forecast_operator import IncidentForecastOperator
from operators.order_forecast_operator import OrderForecastOperator
from operators.utils.resources import ResourcesList

resources_dict = Variable.get('order-forecast-resources',
                              default_var=config['order-forecast']['resources'],
                              deserialize_json=True)
resources = ResourcesList(resources_dict)
maximum_gb_billed_default = int(Variable.get('order-forecast-maximum-gb-billed',
                                             default_var=config['order-forecast']['maximum_gb_billed'],
                                             deserialize_json=False))


def create_bq_table_operator(parent_dag,
                             query_name,
                             task_id=None,
                             params=None,
                             maximum_gb_billed=maximum_gb_billed_default):
    if params is None:
        params = {}

    operator = BigQueryOperator(
        dag=parent_dag,
        task_id=task_id or query_name.replace('/', '_'),
        sql=f"{query_name}.sql",
        params=params,
        use_legacy_sql=False,
        bigquery_conn_id="bigquery_default",
        maximum_bytes_billed=maximum_gb_billed * 1e9,
    )
    return operator


def create_bq_fs_validation_operator(parent_dag,
                                     query_name,
                                     country_code):
    # this operator fails, if the resulting query
    # does not return a single row with values that can be
    # cast to true
    operator = BigQueryCheckOperator(
        dag=parent_dag,
        task_id=f'validate_{query_name}_{country_code}',
        sql=f'datasets/feature_store/validation/{query_name}.sql',
        params={'country_code': country_code},
        use_legacy_sql=False,
        bigquery_conn_id='bigquery_default',
        retries=1,
        execution_timeout=timedelta(minutes=20),
    )
    operator.ui_color = '#86C19C'
    return operator


def create_up_to_date_check_operator(parent_dag,
                                     country_code):
    """Operator to skip export of data if data has not been loaded until
    local midnight for all zones in the given country."""
    operator = ShortCircuitOperator(
        dag=parent_dag,
        task_id=f'check_up_to_date_{country_code}',
        provide_context=False,
        python_callable=country_is_up_to_date,
        op_kwargs={'country_code': country_code},
        retries=1,
        execution_timeout=timedelta(minutes=20),
    )
    operator.ui_color = '#D8BFD8'
    return operator


def create_model_weights_update_check_operator(parent_dag):
    """Operator to make sure model weights are updated regularly
       once a month, but not more often."""
    operator = ShortCircuitOperator(
        dag=parent_dag,
        task_id=f'check_model_weights_need_update',
        provide_context=False,
        python_callable=model_weights_need_update,
        retries=1,
        execution_timeout=timedelta(minutes=20),
    )
    operator.ui_color = '#D8BFD8'
    return operator


def create_forecast_exists_check_operator(parent_dag,
                                          country_code,
                                          model_name,
                                          model_output_bucket,
                                          model_output_path,
                                          date=get_yesterday(),
                                          skip_check=False):
    """Operator to skip forecast if it already exists."""
    operator = ShortCircuitOperator(
        dag=parent_dag,
        task_id=f'check_if_not_exists_{model_name}_{country_code}',
        provide_context=True,
        python_callable=forecast_does_not_exist if not skip_check else lambda **kwargs: True,
        # although op_kwargs should be rendered, in practice it does not work
        # therefore we use templates_dict which can be used as a dict in the callable
        templates_dict={'country_code': country_code,
                        'model_name': model_name,
                        'model_output_bucket': model_output_bucket,
                        'model_output_path': model_output_path,
                        'date': date},
        retries=1,
        execution_timeout=timedelta(minutes=20),
    )
    operator.ui_color = '#D8BFD8'
    return operator


def create_bq_table_validation_operator(parent_dag,
                                        table_name):
    # this operator fails, if the resulting query
    # does not return a single row with values that can be
    # cast to true
    operator = BigQueryCheckOperator(
        dag=parent_dag,
        task_id=f'validate_{table_name}',
        sql=f'validation/{table_name}.sql',
        use_legacy_sql=False,
        bigquery_conn_id='bigquery_default',
        retries=1,
    )
    operator.ui_color = '#86C19C'
    return operator


def create_bqq_to_gcs_fs_operator(parent_dag,
                                  query_name,
                                  country_code,
                                  data_lake_bucket,
                                  feature_store_path,
                                  date=get_yesterday()):
    path = f'gs://{data_lake_bucket}/{feature_store_path}/{date}/{country_code}/{query_name}/data-*.csv.gz'

    operator = BigQueryExportQueryToGCSOperator(
        dag=parent_dag,
        task_id=f'export_gcs_{query_name}_{country_code}',
        sql=f'datasets/feature_store/{query_name}.sql',
        params={'country_code': country_code},
        destination_cloud_storage_uris=[path],
        compression='GZIP',
        export_format='CSV',
        field_delimiter=';',
        print_header=True,
        bigquery_conn_id='bigquery_default',
        execution_timeout=timedelta(minutes=20),
    )

    operator.ui_color = '#cce6ff'
    return operator


def create_gcs_to_s3_operator(parent_dag,
                              query_name,
                              country_code,
                              data_lake_bucket,
                              feature_store_path,
                              date=get_yesterday()):

    # see comment above
    prefix = f'{feature_store_path}/{date}/{country_code}/{query_name}/'

    operator = GoogleCloudStorageToS3Operator(
        dag=parent_dag,
        task_id=f'export_s3_{query_name}_{country_code}',
        bucket=data_lake_bucket,
        prefix=prefix,
        # file ending filter
        delimiter='.csv.gz',
        google_cloud_storage_conn_id='bigquery_default',
        dest_aws_conn_id='aws_default',
        # trailing slash is needed
        dest_s3_key=f's3://{data_lake_bucket}/',
        replace=True,
        execution_timeout=timedelta(minutes=20),
    )
    operator.ui_color = '#E19999'
    return operator


def create_order_forecast_operator(parent_dag,
                                   country_code,
                                   model_parameters,
                                   version,
                                   data_lake_bucket,
                                   feature_store_path,
                                   model_output_bucket,
                                   model_output_path,
                                   date=get_yesterday(),
                                   run_date=get_run_date(),
                                   skip_sentry=False):
    model_name = model_parameters["model_name"]
    model_name_label = model_name.replace('.', '_')

    s3_features_output_path = f'{feature_store_path}/{date}/{country_code}/'
    s3_forecast_output_folder = f'{model_output_path}/{date}/{country_code}/{model_name}/'

    model_parameters['run_date'] = run_date

    operator = OrderForecastOperator(
        task_id=f'run_forecast_{model_name_label}_{country_code}',
        country_code=country_code,
        s3_input_bucket=data_lake_bucket,
        s3_input_folder=s3_features_output_path,
        s3_output_bucket=model_output_bucket,
        s3_output_folder=s3_forecast_output_folder,
        cmds='python3',
        entrypoint='runner/run_forecast_model.py',
        parameters=model_parameters,
        version=version,
        skip_sentry=skip_sentry,
        execution_timeout=timedelta(hours=10),
        dag=parent_dag,
        resources=resources.get(country_code),
        pool=config['pools']['forecast_run_models']['name'],
    )
    return operator


def create_incident_forecast_operator(parent_dag,
                                      task_id,
                                      parameters,
                                      cmds,
                                      entrypoint,
                                      on_success_callback=alerts.setup_callback(),
                                      on_failure_callback=alerts.setup_callback(),
                                      skip_sentry=False):

    operator = IncidentForecastOperator(
        cmds=cmds,
        entrypoint=entrypoint,
        parameters=parameters,
        execution_timeout=timedelta(hours=10),
        dag=parent_dag,
        task_id=task_id,
        resources=resources.get(parameters['country_iso'] if 'country_iso' in parameters else None),
        pool=config['pools']['incident_forecast_run_models']['name'],
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        skip_sentry=skip_sentry
    )
    operator.ui_color = '#f7f13b'

    return operator


def create_order_forecast_imputation_operator(parent_dag,
                                              country_code,
                                              version,
                                              data_lake_bucket,
                                              feature_store_path,
                                              date=get_yesterday(),
                                              run_date=get_run_date()):
    s3_features_input_path = f'{feature_store_path}/{date}/{country_code}/'
    s3_features_output_path = f'{feature_store_path}/{date}/{country_code}/'

    parameters = {'run_date': run_date}

    operator = OrderForecastOperator(
        task_id=f'run_imputation_{country_code}',
        country_code=country_code,
        s3_input_bucket=data_lake_bucket,
        s3_input_folder=s3_features_input_path,
        s3_output_bucket=data_lake_bucket,
        s3_output_folder=s3_features_output_path,
        cmds='Rscript',
        entrypoint='phoenix/script.R',
        parameters=parameters,
        version=version,
        execution_timeout=timedelta(hours=2),
        dag=parent_dag,
        resources=resources.get_default(),
    )
    return operator


def create_order_distribution_builder_operator(parent_dag,
                                               country_code,
                                               version,
                                               data_lake_bucket,
                                               feature_store_path,
                                               date=get_yesterday(),
                                               run_date=get_run_date()):
    s3_features_input_path = f'{feature_store_path}/{date}/{country_code}/'
    s3_features_output_path = f'{feature_store_path}/{date}/{country_code}/'

    parameters = {'run_date': run_date}

    operator = OrderForecastOperator(
        task_id=f'run_order_distribution_builder_{country_code}',
        country_code=country_code,
        s3_input_bucket=data_lake_bucket,
        s3_input_folder=s3_features_input_path,
        s3_output_bucket=data_lake_bucket,
        s3_output_folder=s3_features_output_path,
        cmds='python3',
        entrypoint='order_distributions/run_order_distribution_builder.py',
        parameters=parameters,
        version=version,
        execution_timeout=timedelta(hours=2),
        dag=parent_dag,
        resources=resources.get_default(),
    )
    return operator


def create_s3_to_gcs_operator(parent_dag,
                              country_code,
                              model_output_bucket,
                              model_output_path,
                              model_name,
                              date=get_yesterday()):
    gcs_destination = f'gs://{model_output_bucket}/'
    s3_forecast_output_folder = f'{model_output_path}/{date}/{country_code}/{model_name}/'
    operator = S3ToGoogleCloudStorageOperator(
        task_id=f'export_gcs_{model_name}_{country_code}',
        bucket=model_output_bucket,
        prefix=s3_forecast_output_folder,
        dest_gcs_conn_id='google_cloud_default',
        dest_gcs=gcs_destination,
        replace=True,
        dag=parent_dag,
        execution_timeout=timedelta(minutes=20),
    )
    return operator


def create_gcs_to_bq_operator(parent_dag,
                              country_code,
                              model_output_bucket,
                              model_output_path,
                              model_name,
                              date=get_yesterday()):
    s3_forecast_output_folder = f'{model_output_path}/{date}/{country_code}/{model_name}/forecasts.csv.gz'

    operator = GoogleCloudStorageToBigQueryOperator(
        task_id=f'load_bq_{model_name}_{country_code}',
        bucket=model_output_bucket,
        source_objects=[s3_forecast_output_folder],
        schema_fields=get_forecast_output_schema(),
        destination_project_dataset_table='ds_model_outputs.order_forecasts',
        autodetect=False,
        skip_leading_rows=1,
        compression='GZIP',
        field_delimiter=';',
        source_format='CSV',
        time_partitioning={'type': 'DAY', 'field': 'forecast_date'},
        src_fmt_configs={'nullMarker': 'NA'},
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        ignore_unknown_values=True,
        dag=parent_dag,
        execution_timeout=timedelta(minutes=20),
    )

    return operator


def generate_message_payload(country_code: str,
                             bucket: str = 'production-eu-data-science-model-outputs',
                             folder: str = 'order_forecast_output',
                             date: str = get_yesterday(),
                             model_name: str = 'production',
                             request_id: str = None,
                             forecast_type: str = 'orders_forecast'):
    path = f'{folder}/{date}/{country_code}/{model_name}/'

    if not request_id:
        ts = str(datetime.now().strftime("%Y%m%d%H%M%S"))
        request_id = f'{country_code}/{ts}'

    payload = {
        'country_code': country_code,
        'bucket': bucket,
        'key': path,
        'request_id': request_id,
        'forecast_type': forecast_type,
    }
    return payload


def create_sqs_publish_operator(parent_dag,
                                country_code,
                                model_output_bucket,
                                model_output_path,
                                queue_url=None,
                                date=get_yesterday(),
                                model_name='production'):
    payload = generate_message_payload(country_code=country_code,
                                       bucket=model_output_bucket,
                                       folder=model_output_path,
                                       date=date,
                                       model_name=model_name)

    # still generate old payload for downward compat, then combine
    old_payload = {
        's3': {
            'bucket': {
                'name': model_output_bucket
            },
            'object': {
                'key': f'{model_output_path}/{date}/{country_code}/{model_name}/manifest.json'
            }
        }
    }
    combined_payload = {**payload, **old_payload}

    region = get_region(country_code)

    if not queue_url:
        queue_url = get_queue_url(region)

    operator = SQSPublishOperator(
        task_id=f'publish_model_output_sqs_{country_code}',
        sqs_queue=queue_url,
        message_content=json.dumps(combined_payload),
        dag=parent_dag,
        # TODO: aws_conn_id & boto3 auth are somewhat region bound due to the extra-config
        aws_conn_id=get_aws_conn_id(region),
        execution_timeout=timedelta(minutes=20),
    )

    return operator
