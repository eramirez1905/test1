import json
from datetime import timedelta

from airflow import configuration
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

from configuration import config
from datahub.common import alerts
from datahub.dwh_import_read_replica import DwhImportReadReplica
from datahub.operators.delete_file_google_cloud_storage_operator import DeleteFileToGoogleCloudStorageOperator
from operators.salesforce_to_google_cloud_storage import SalesforceToGoogleCloudStorageOperator

dwh_import_read_replica = DwhImportReadReplica(config)


class SalesforceImport:

    def __init__(self,
                 dwh_import: DwhImportReadReplica,
                 backfill=False):
        self.backfill = backfill
        self.dwh_import = dwh_import

    def __salesforce_import(self, start):
        def get_schema_filename(tablename):
            return f'{configuration.get("core", "dags_folder")}/salesforce/json/{tablename}.json'

        def get_query_filename(tablename):
            return f'{configuration.get("core", "dags_folder")}/salesforce/soql/{tablename}.soql'

        def get_executor_config(backfill):
            if backfill:
                backfill_config = {
                    'KubernetesExecutor': {
                        'request_cpu': '500m',
                        'limit_cpu': '500m',
                        'request_memory': "2000Mi",
                        'limit_memory': "2000Mi",
                    }
                }
                return backfill_config

        dataset = config.get('bigquery').get('dataset').get('raw')
        gcs_bucket_name = config.get("salesforce").get("bucket_name")
        for instance in config.get('salesforce').get('instances'):
            for database in self.dwh_import.databases.values():
                if database.name == f'salesforce_{instance}':
                    for table in database.tables:
                        gcs_object_name = f'salesforce/{table.table_name}/{{{{ ts_nodash }}}}/data.json'
                        with open(get_query_filename(table.table_name)) as f:
                            query = f.read()
                        salesforce_to_gcs = SalesforceToGoogleCloudStorageOperator(
                            instance=instance,
                            salesforce_conn_id=f'salesforce_{instance}',
                            task_id=f'import-to-gcs-{table.table_name}',
                            execution_date="{{ts}}",
                            params={
                                'backfill': self.backfill,
                            },
                            query=query,
                            gcs_bucket_name=gcs_bucket_name,
                            gcs_object_name=gcs_object_name,
                            execution_timeout=timedelta(minutes=30),
                            on_failure_callback=alerts.setup_callback(),
                            executor_config=get_executor_config(self.backfill)
                        )
                        with open(get_schema_filename(table.table_name)) as f:
                            schema = json.load(f)
                        gcs_to_big_query = GoogleCloudStorageToBigQueryOperator(
                            task_id=f'gcs-to-bq-{table.table_name}',
                            bucket=gcs_bucket_name,
                            source_objects=[gcs_object_name],
                            source_format='NEWLINE_DELIMITED_JSON',
                            destination_project_dataset_table=f'{dataset}.{table.table_name}',
                            schema_fields=schema,
                            skip_leading_rows=0,
                            time_partitioning=dwh_import_read_replica.get_time_partitioning_column(table),
                            cluster_fields=dwh_import_read_replica.get_cluster_fields(table),
                            create_disposition='CREATE_IF_NEEDED',
                            write_disposition='WRITE_APPEND',
                            schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
                            execution_timeout=timedelta(minutes=30),
                            on_failure_callback=alerts.setup_callback(),
                            max_bad_records=0,
                        )
                        delete_file = DeleteFileToGoogleCloudStorageOperator(
                            task_id=f'delete-gcs-export-file-{table.table_name}',
                            bucket=gcs_bucket_name,
                            filename=gcs_object_name,
                            on_failure_callback=alerts.setup_callback(),
                        )
                        start >> salesforce_to_gcs >> gcs_to_big_query >> delete_file

    def render(self):
        start = DummyOperator(task_id='start')
        self.__salesforce_import(start=start)
