import json
from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

import dwh_import
from configuration import config
from datahub.common import alerts
from datahub.dwh_import_read_replica import DwhImportReadReplica
from operators.jira_to_google_cloud_storage_operator import JiraToGcsOperator

version = 1
default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2020, 4, 7, 0, 0, 0),
    'retries': 3,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

doc_md = "#### JIRA Importer DAG."


def get_schema_filename():
    return '{}/atlassian/schema.json'.format(configuration.get('core', 'dags_folder'))


dwh_import_read_replica = DwhImportReadReplica(config)

with DAG(
        dag_id=f'jira-importer-v{version}',
        description='DAG which imports issues from JIRA to BigQuery.',
        schedule_interval='30 8 * * *',
        default_args={**dwh_import.DEFAULT_ARGS, **default_args},
        tags=[dwh_import.DEFAULT_ARGS['owner'], 'dwh'],
        catchup=False) as dag:
    dag.doc_md = doc_md

    dataset = config.get('bigquery').get('dataset').get('raw')
    gcs_bucket_name = config.get('jira').get('bucket_name')
    jira_projects = map(str.strip, config.get('jira').get('jira_projects').split(","))
    issue_fields = config.get('jira').get('issue_fields')

    start = DummyOperator(
        task_id='start',
        on_failure_callback=alerts.setup_callback(),
    )

    with open(get_schema_filename()) as f:
        schema = json.load(f)

    tasks = []

    for jira_project in jira_projects:
        gcs_object_path = f'jira_importer/{jira_project}/{{{{ ts_nodash }}}}'
        gcs_object_name = f'{gcs_object_path}/issues.json'
        import_project = JiraToGcsOperator(
            dag=dag,
            task_id=f'import-jira-issues-{jira_project}',
            jira_project=jira_project,
            issue_fields=issue_fields,
            gcs_bucket_name=gcs_bucket_name,
            gcs_object_name=gcs_object_name,
            execution_timeout=timedelta(minutes=30),
            on_failure_callback=alerts.setup_callback(),
        )

        for database in dwh_import_read_replica.databases.values():
            if database.name == 'jira':
                for table in database.tables:
                    gcs_to_big_query = GoogleCloudStorageToBigQueryOperator(
                        task_id=f'gcs-to-bq-{jira_project}',
                        bucket=gcs_bucket_name,
                        source_objects=[gcs_object_name],
                        source_format='NEWLINE_DELIMITED_JSON',
                        destination_project_dataset_table=f'{dataset}.{table.table_name}',
                        schema_fields=schema,
                        time_partitioning=dwh_import_read_replica.get_time_partitioning_column(table),
                        cluster_fields=dwh_import_read_replica.get_cluster_fields(table),
                        create_disposition='CREATE_IF_NEEDED',
                        write_disposition='WRITE_APPEND',
                        schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
                        execution_timeout=timedelta(minutes=30),
                        on_failure_callback=alerts.setup_callback(),
                        max_bad_records=0,
                        autodetect=False,
                        ignore_unknown_values=True,
                    )

        start >> import_project >> gcs_to_big_query
