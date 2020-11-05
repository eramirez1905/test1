from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from airflow import DAG, configuration
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from datahub.operators.gcs_hurrier_operator import GoogleCloudStorageHurrierOperator
from datahub.operators.bigquery_export_query_to_gcs_operator import BigQueryExportQueryToGCSOperator

from configuration import config

version = 1
dags_folder = configuration.get('core', 'dags_folder')

schedule = '0 12 * * *'

model_output_bucket = Variable.get('data-science-model-outputs-bucket',
                                   default_var='staging-eu-data-science-model-outputs')
feature_store_path = 'starting_point_split_suggestions'
token = Variable.get('hurrier-api-token', default_var='skip')

default_args = {
    'owner': 'data-science',
    'start_date': datetime(2019, 9, 12),
    'retries': 2,
    'retry_delay': timedelta(minutes=20),
    'max_active_runs': 1,
    'execution_timeout': timedelta(hours=3),
    'executor_config': {
        'KubernetesExecutor': {
            'request_cpu': "200m",
            'limit_cpu': "200m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi",
        },
    },
}


def generate_country_query(table_name, country_code):
    return f"SELECT * FROM {table_name} WHERE country_code = '{country_code}'"


def generate_object_name_template(country_code):
    return f'{feature_store_path}/{{{{ds}}}}/{country_code}/output.csv'


def create_upload_split_suggestions_operator(parent_dag, country_code):
    object_name = generate_object_name_template(country_code)

    def data_parser(x):
        """Function to read data from GCS with."""
        return pd.read_csv(x, sep=';')

    def data_transformer(x):
        """"Function to transform data from GCS into API payload."""
        records = x.to_dict('records')
        starting_point_splits = [
            {
                'id': int(record['starting_point_id']),
                'suggested_split': float(np.nan_to_num(record['split_suggested'])),
            } for record in records
        ]
        payload = {'starting_points': starting_point_splits}
        return payload

    operator = GoogleCloudStorageHurrierOperator(
        bucket_name=model_output_bucket,
        object_name=object_name,
        country_code=country_code,
        endpoint='api/forecast/v1/starting-points/bulk',
        data_parser=data_parser,
        data_transformer=data_transformer,
        http_method='PUT',
        dag=parent_dag,
        task_id=f'upload_data_{country_code}',
    )
    return operator


dag = DAG(
    dag_id=f'staffing-starting-point-split-suggestions-v{version}',
    description='Compute rooster starting point split suggestions from BigQuery data, '
                + 'upload to GCS and and post to forecast API.',
    schedule_interval=schedule,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath=f'{dags_folder}/rooster',
    catchup=False,
    tags=[default_args['owner']],
)

start_operator = DummyOperator(dag=dag, task_id='start')

create_table_operator = BigQueryOperator(
    dag=dag,
    task_id='create_bq_table',
    sql=f'starting_point_split_suggestions.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default',
)

start_operator >> create_table_operator

for region in config['regions']:
    start_export_region_operator = DummyOperator(dag=dag, task_id=f'start_export_{region}')
    create_table_operator >> start_export_region_operator

    for country_code in config['regions'][region]['countries']:
        export_data_operator = BigQueryExportQueryToGCSOperator(
            dag=dag,
            task_id=f'export_data_{country_code}',
            sql=generate_country_query('staffing.starting_point_split_suggestions', country_code),
            destination_cloud_storage_uris=[f'gs://{model_output_bucket}/'
                                            + generate_object_name_template(country_code)],
            export_format='CSV',
            compression='NONE',
            field_delimiter=';',
            print_header=True,
            bigquery_conn_id='bigquery_default',
        )
        upload_split_suggestions_operator = create_upload_split_suggestions_operator(parent_dag=dag, country_code=country_code)

        start_export_region_operator >> export_data_operator >> upload_split_suggestions_operator
