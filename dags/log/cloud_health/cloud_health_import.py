from datetime import timedelta

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

from configuration import config
from datahub.common import alerts
from datahub.dwh_import_read_replica import DwhImportReadReplica
from datahub.operators.delete_file_google_cloud_storage_operator import DeleteFileToGoogleCloudStorageOperator
from operators.cloud_health_to_google_cloud_storage_operator import CloudHealthOperatorToGoogleCloudStorageOperator


class CloudHealthImport:
    def __init__(self,
                 project_id: str,
                 dataset_id: str,
                 gcs_bucket_name: str,
                 dwh_import: DwhImportReadReplica,
                 is_backfill=False):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.gcs_bucket_name = gcs_bucket_name
        self.is_backfill = is_backfill
        self.dwh_import = dwh_import

    def render(self):
        start = DummyOperator(
            task_id='start',
        )

        for report in config.get('cloud_health').get('reports').get('standards'):
            report_name = f"{report['type']}-{report['name']}"
            gcs_object_name = f'cloud_health_export/report_{report_name}_{{{{ ts_nodash }}}}.json'
            import_reports_operator = CloudHealthOperatorToGoogleCloudStorageOperator(
                task_id=f'import-to-gcs-{report_name}',
                report=report,
                gcs_bucket_name=self.gcs_bucket_name,
                gcs_object_name=gcs_object_name,
                execution_timeout=timedelta(minutes=60),
                on_failure_callback=alerts.setup_callback(),
                is_backfill=self.is_backfill,
            )

            delete_file = DeleteFileToGoogleCloudStorageOperator(
                task_id=f'delete-gcs-export-file-{report_name}',
                bucket=self.gcs_bucket_name,
                filename=gcs_object_name,
                on_failure_callback=alerts.setup_callback(),
            )

            for database in self.dwh_import.databases.values():
                if database.name == 'cloud_health':
                    for table in database.tables:
                        gcs_to_big_query = GoogleCloudStorageToBigQueryOperator(
                            task_id=f'gcs-to-bq-{report_name}',
                            bucket=self.gcs_bucket_name,
                            source_objects=[gcs_object_name],
                            source_format='NEWLINE_DELIMITED_JSON',
                            destination_project_dataset_table=f'{self.project_id}.{self.dataset_id}.{table.table_name}',
                            skip_leading_rows=0,
                            time_partitioning=DwhImportReadReplica.get_time_partitioning_column(table),
                            cluster_fields=DwhImportReadReplica.get_cluster_fields(table),
                            create_disposition='CREATE_IF_NEEDED',
                            write_disposition='WRITE_APPEND',
                            schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
                            execution_timeout=timedelta(minutes=30),
                            on_failure_callback=alerts.setup_callback(),
                            max_bad_records=0,
                        )

                        start >> import_reports_operator >> gcs_to_big_query >> delete_file
