from datetime import timedelta, datetime

from airflow import DAG, configuration
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from configuration import config
from datahub.common import alerts
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator
from operators.utils.resources import ResourcesList

default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 12),
    'retries': 1,
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

resources_dict = Variable.get('location-update-models-resources',
                              default_var=config['location-update-models']['resources'],
                              deserialize_json=True)
resources = ResourcesList(resources_dict)

schedule = '0 2 * * *'

VERSION = 1
DOCKER_IMAGE = '683110685365.dkr.ecr.eu-west-1.amazonaws.com/location-update-models:latest'

DATA_LAKE_BUCKET = Variable.get('data-science-data-lake-bucket',
                                default_var='staging-eu-data-science-data-lake')
MODEL_OUTPUTS_BUCKET = Variable.get('data-science-model-outputs-bucket',
                                    default_var='staging-eu-data-science-model-outputs')
DEFAULT_ENV_VARS = {
    "DATA_LAKE_BUCKET": DATA_LAKE_BUCKET,
    "MODEL_OUTPUTS_BUCKET": MODEL_OUTPUTS_BUCKET
}

# Env vars configurable via airflow ui.
VARIABLE_ENV_VARS = Variable.get("location-update-models-env-vars", default_var={}, deserialize_json=True)

# The variable env vars will overwrite the default ones.
ENV_VARS = {**DEFAULT_ENV_VARS, **VARIABLE_ENV_VARS}

template_search_path = '{}/data_fridge_import/sql'.format(configuration.get('core', 'dags_folder'))

dag = DAG(
    dag_id=f'location-update-models-v{VERSION}',
    description='Updates vendor/customer locations based on pickup/dropoff data',
    default_args=default_args,
    catchup=False,
    # run every day at 2am
    schedule_interval=schedule,
    template_searchpath=template_search_path,
    tags=[default_args['owner']],
)

gcs_bucket_name = config.get("location-updates").get("bucket_name")
s3_bucket_name = config.get("location-updates").get("s3_bucket")

start_operator = DummyOperator(
    dag=dag,
    task_id=f'start'
)

end_operator = DummyOperator(
    dag=dag,
    task_id=f'end'
)

for region in config["regions"]:

    start_region_operator = DummyOperator(
        dag=dag,
        task_id=f'start_{region}'
    )
    start_operator >> start_region_operator

    end_region_operator = DummyOperator(
        dag=dag,
        task_id=f'end_{region}'
    )

    countries = config["regions"][region]["countries"]

    for country in countries:

        # Pickups
        run_pickup_models_operator = LogisticsKubernetesOperator(
            task_id=f'run-pickup-models-{country}',
            name=f'run-pickup-models-{country}',
            image=DOCKER_IMAGE,
            cmds=['python3.7', 'cli.py', 'run-pickup-models'],
            node_profile=config.get("location-updates").get('node_profile', 'simulator-node'),
            env_vars=ENV_VARS,
            arguments=[
                '--execution-date', '{{next_ds}}',
                '--country-code', country
            ],
            resources=resources.get(country),
            dag=dag
        )
        run_pickup_models_operator.ui_color = '#FDF0D5'

        # Dropoffs
        run_dropoff_models_operator = LogisticsKubernetesOperator(
            task_id=f'run-dropoff-models-{country}',
            name=f'run-dropoff-models-{country}',
            image=DOCKER_IMAGE,
            cmds=['python3.7', 'cli.py', 'run-dropoff-models'],
            node_profile=config.get("location-updates").get('node_profile', 'simulator-node'),
            env_vars=ENV_VARS,
            arguments=[
                '--execution-date', '{{next_ds}}',
                '--country-code', country
            ],
            resources=resources.get(country),
            dag=dag
        )
        run_dropoff_models_operator.ui_color = '#ADB7D7'

        (start_region_operator
         >> run_pickup_models_operator
         >> run_dropoff_models_operator
         >> end_region_operator)

    end_region_operator >> end_operator

gcs_connection_id = 'google_cloud_default'
aws_connection_id = 'aws_s3_external_dh'

export_config = {
    "customer": {
        "bucket_file_prefix": "customer_location_update",
        "table": "location_updates_customers"
    },
    "vendor": {
        "bucket_file_prefix": "vendor_location_update",
        "table": "location_updates_vendors",
    }
}


def getBigQueryDestination(table_name):
    return f"staging.{{{{ dag.dag_id.replace('-', '_') }}}}_{table_name}_{{{{ ts_nodash }}}}"


for conf_key, val in export_config.items():
    table_name = val["table"]
    file_prefix = f"{val['bucket_file_prefix']}/date={{{{ next_ds }}}}/{table_name}"

    pre_delete_gcs_folder_operator = GoogleCloudStorageDeleteOperator(
        bucket_name=gcs_bucket_name,
        dag=dag,
        google_cloud_storage_conn_id=gcs_connection_id,
        on_failure_callback=alerts.setup_callback(),
        prefix=file_prefix,
        task_id=f'pre_delete_{conf_key}_location_update_from_gcs',
    )

    bq_operator = BigQueryOperator(
        bigquery_conn_id='bigquery_default',
        create_disposition='CREATE_IF_NEEDED',
        execution_timeout=timedelta(minutes=15),
        write_disposition='WRITE_TRUNCATE',
        dag=dag,
        destination_dataset_table=getBigQueryDestination(table_name),
        on_failure_callback=alerts.setup_callback(),
        sql=f'{table_name}.sql',
        task_id=f'export_table_{table_name}',
    )

    export_to_gcs_operator = BigQueryToCloudStorageOperator(
        task_id=f'export_to_gcs_{table_name}',
        source_project_dataset_table=getBigQueryDestination(table_name),
        destination_cloud_storage_uris=[
            f"gs://{gcs_bucket_name}/{file_prefix}_{{{{macros.time.strftime('%Y%m%d_%H%M%S')}}}}.json.gz"],
        compression='GZIP',
        bigquery_conn_id='bigquery_default',
        export_format='NEWLINE_DELIMITED_JSON',
        dag=dag,
        on_failure_callback=alerts.setup_callback(),
    )

    gcs_to_s3_operator = GoogleCloudStorageToS3Operator(
        dag=dag,
        task_id=f'copy_to_s3_{table_name}',
        dest_aws_conn_id=aws_connection_id,
        google_cloud_storage_conn_id=gcs_connection_id,
        bucket=gcs_bucket_name,
        prefix=file_prefix,
        delimiter="gz",
        dest_s3_key=f's3://{s3_bucket_name}/',
        replace=True,
        on_failure_callback=alerts.setup_callback(),
    )

    post_delete_gcs_folder_operator = GoogleCloudStorageDeleteOperator(
        bucket_name=gcs_bucket_name,
        dag=dag,
        google_cloud_storage_conn_id=gcs_connection_id,
        on_failure_callback=alerts.setup_callback(),
        prefix=file_prefix,
        task_id=f'post_delete_{conf_key}_location_update_from_gcs',
    )

    # Add the tasks for exporting location data to DAG
    end_operator >> pre_delete_gcs_folder_operator >> bq_operator >> export_to_gcs_operator >> gcs_to_s3_operator >> post_delete_gcs_folder_operator
