import os
from datetime import timedelta
from functools import lru_cache
from typing import Dict, List

from airflow import DAG, AirflowException, LoggingMixin
from airflow.contrib.operators.gcp_transfer_operator import S3ToGoogleCloudStorageTransferOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.contrib.operators.s3_to_gcs_operator import S3ToGoogleCloudStorageOperator
from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

from datahub.common import alerts
from datahub.common.helpers import TableSettings, DataSource, DatabaseSettings, DatabaseType, create_task_id
from datahub.common.singleton import Singleton
from datahub.curated_data.metadata_execution_table import MetadataExecutionTable
from datahub.operators.bigquery.bigquery_authorized_view_operator import BigQueryAuthorizeViewOperator
from datahub.operators.bigquery.bigquery_check_operator import BigQueryCheckOperator
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from datahub.operators.bigquery.bigquery_table_patch_operator import BigQueryTablePatchOperator
from datahub.operators.bigquery.bigquery_update_metadata_execution_operator import \
    BigQueryUpdateMetadataExecutionOperator
from datahub.operators.databricks.databricks_export_read_replica_to_bigquery_operator import \
    DatabricksExportReadReplicaToBigQueryOperator
from datahub.operators.dataproc.dataproc_create_cluster_operator import \
    DataprocCreateClusterOperator
from datahub.operators.dataproc.dataproc_delete_cluster_operator import \
    DataprocDeleteClusterOperator
from datahub.operators.dataproc.dataproc_export_read_replica_to_bigquery_operator import \
    DataprocExportReadReplicaToBigQueryOperator
from datahub.operators.debezium.kafka_connect_bigquery_sink_operator import KafkaConnectBigQuerySinkOperator
from datahub.operators.debezium.kafka_connect_mysql_source_operator import KafkaConnectMysqlSourceOperator
from datahub.operators.redshift.redshift_to_s3_iam_operator import RedshiftToS3IamOperator
from datahub.operators.s3.s3_check_object_exists_operator import S3CheckObjectExistsOperator, S3_OBJECT_EXISTS


class Pipeline:
    def __init__(self, head: BaseOperator, tail: BaseOperator) -> None:
        self.head = head
        self.tail = tail


class DwhImportReadReplica(LoggingMixin, metaclass=Singleton):

    def __init__(self, config: dict):
        super().__init__()
        self.config = config
        self.staging = self.config.get('bigquery').get('dataset').get('staging')
        self.dataset = self.config.get('bigquery').get('dataset').get('raw')
        self.ml_dataset = self.config.get('bigquery').get('dataset').get('ml')
        self.dl_dataset = self.config.get('bigquery').get('dataset').get('dl')
        self.hl_dataset = self.config.get('bigquery').get('dataset').get('hl')
        self.databases_config = config['dwh_merge_layer_databases']
        self.bigquery_flat = self.config.get('bigquery').get('bigquery_conn_id_flat', 'bigquery_default')
        self.bigquery_flat_pool = self.config.get('pools').get('bigquery_flat',
                                                               {'name': 'export_read_replica_misc'}).get('name')

    @property
    @lru_cache(maxsize=None)
    def databases(self) -> Dict[str, DatabaseSettings]:
        return {name: DatabaseSettings(name, params) for name, params in self.databases_config.items()}

    @property
    @lru_cache(maxsize=None)
    def tables(self) -> Dict[str, TableSettings]:
        tables = {}
        for database in self.databases.values():
            for table in database.tables:
                if table.table_name in tables:
                    raise AirflowException(f"Table {table.table_name} already defined.")
                tables[table.table_name] = table
        return tables

    @property
    def metadata_execution_table(self):
        return MetadataExecutionTable()

    def read_query_file(self, filename):
        operator_path = os.path.dirname(os.path.realpath(__file__))
        with open(f'{operator_path}/sql/{filename}') as f:
            return f.read()

    def import_from_read_replicas(self, dag: DAG, table: TableSettings):
        dataset = self.dataset

        current_project_id = self.config.get('bigquery').get('project_id')
        staging_project_id = self.config.get('bigquery').get('staging_project_id')

        import_pipeline = self.create_read_replica_operators(dag, table, current_project_id, dataset)
        export_read_replica = import_pipeline.head
        copy_staging_table_to_raw_layer = import_pipeline.tail

        metadata_execution_operator = self.create_update_metadata_execution_operator(current_project_id, dag, dataset,
                                                                                     table)
        copy_staging_table_to_raw_layer >> metadata_execution_operator

        if table.create_maintenance_tasks:
            short_circuit_operator = ShortCircuitOperator(
                dag=dag,
                task_id=create_task_id(f'maint-{table.table_name}'),
                provide_context=True,
                python_callable=self.is_execution_date_next_day,
                pool='export_read_replica_misc',
            )

            maintenance_operators: List[BaseOperator] = self.create_maintenance_operators(
                current_project_id,
                dag,
                table,
                staging_project_id,
                dataset
            )

            copy_staging_table_to_raw_layer >> short_circuit_operator >> maintenance_operators

        return export_read_replica

    def create_maintenance_operators(self,
                                     project_id,
                                     dag: DAG,
                                     table: TableSettings,
                                     staging_project_id,
                                     dataset: str) -> List[BaseOperator]:
        operators = []
        if not table.keep_last_import_only:
            cleanup_table = BigQueryOperator(
                dag=dag,
                task_id=create_task_id(f'cleanup-{table.table_name}'),
                bigquery_conn_id=self.bigquery_flat,
                params={
                    'project_id': project_id,
                    'dataset': dataset,
                    'table_name': table.table_name,
                    'pk_columns': table.pk_columns,
                    'extra_partition_columns': self.get_extra_partition_columns(table)
                },
                sql=self.read_query_file('cleanup/remove_duplicates.sql'),
                destination_dataset_table=f'{project_id}.{dataset}.{table.table_name}',
                time_partitioning=self.get_time_partitioning_column(table),
                cluster_fields=self.get_cluster_fields(table),
                use_legacy_sql=False,
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE',
                execution_timeout=timedelta(minutes=15),
                on_failure_callback=alerts.setup_callback(),
                enabled=table.cleanup_enabled,
                ui_color='#eb5946',
                ui_fgcolor='#FFF',
                pool=self.bigquery_flat_pool,
            )
            operators.append(cleanup_table)

        patch_table = BigQueryTablePatchOperator(
            dag=dag,
            task_id=create_task_id(f'patch-{table.table_name}'),
            dataset_id=dataset,
            table_id=table.table_name,
            enabled=table.require_partition_filter,
            require_partition_filter=table.require_partition_filter,
            pool='export_read_replica_misc',
        )
        operators.append(patch_table)

        if table.create_views:
            operators.extend(self.create_views(project_id, dag, dataset, table))
            operators.extend(self.create_views(project_id, dag, dataset, table, target_project_id=staging_project_id))

        start_create_views = DummyOperator(
            dag=dag,
            task_id=create_task_id(f'misc-{table.table_name}')
        )
        start_create_views >> operators

        start_sanity_check = self.create_sanity_check_operators(project_id, dag, table, dataset)

        return [start_sanity_check, start_create_views]

    def create_update_metadata_execution_operator(self, project_id, dag, dataset, table):
        return BigQueryUpdateMetadataExecutionOperator(
            project_id=project_id,
            dataset_id=dataset,
            table_name=table.table_name,
            metadata_execution_table=self.metadata_execution_table,
            dag=dag,
            task_id=create_task_id(f'metadata-{table.table_name}'),
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
            pool='export_read_replica_misc',
        )

    def create_read_replica_operators(self, dag: DAG, table: TableSettings, project_id: str, dataset: str) -> Pipeline:
        head_task_id = create_task_id(f'import-{table.table_name}')
        if table.source == DataSource.RDS:
            return self.import_rds_tables(dag, dataset, head_task_id, project_id, table)
        if table.source == DataSource.CLOUD_SQL:
            return self.import_gcp_rds_tables(dag, dataset, head_task_id, project_id, table)
        if table.source == DataSource.Debezium:
            return self.import_debezium_tables(dag, dataset, project_id, table)
        elif table.source in [DataSource.RedShift_S3]:
            return self.import_redshift_s3_tables(dag, dataset, head_task_id, project_id, table)
        elif table.source in [DataSource.S3]:
            return self.import_s3_tables(dag, dataset, head_task_id, project_id, table)
        elif table.source in [DataSource.Redshift]:
            return self.import_redshift_tables(dag, dataset, head_task_id, project_id, table)
        elif table.source in [DataSource.Firebase, DataSource.GoogleAnalytics, DataSource.DynamoDB, DataSource.SQS,
                              DataSource.API, DataSource.BigQuery, DataSource.KinesisStream]:
            export_read_replica = DummyOperator(
                dag=dag,
                task_id=head_task_id
            )

            return Pipeline(export_read_replica, export_read_replica)
        else:
            raise AirflowException(
                f'Import Operator does not exist for table {table.table_name} with source={table.source}')

    def import_debezium_tables(self, dag: DAG, dataset: str, project_id: str, table: TableSettings) -> Pipeline:
        task_id = create_task_id(f"import-{table.database.name}")
        if dag.has_task(task_id):
            source = dag.get_task(task_id)
        else:
            if table.database.type == DatabaseType.mysql:
                source = KafkaConnectMysqlSourceOperator(
                    dag=dag,
                    task_id=task_id,
                    table=table,
                    database=table.database,
                    project_id=project_id,
                    dataset_id=dataset,
                    broker_endpoints=self.config.get('debezium').get('broker_endpoints')
                )
            else:
                raise AirflowException(f"Database type {table.database.type} not supported for DataSource Debezium")

        sink = KafkaConnectBigQuerySinkOperator(
            dag=dag,
            task_id=create_task_id(f"sink-{table.table_name}"),
            database=table.database,
            table=table,
            project_id=project_id,
            dataset_id=dataset,
            schema_registry_location=self.config.get('debezium').get('schema_registry_location')
        )
        sink << source

        return Pipeline(source, sink)

    def import_rds_tables(self, dag: DAG, dataset: str, head_task_id: str, project_id: str,
                          table: TableSettings) -> Pipeline:
        export_read_replica = DatabricksExportReadReplicaToBigQueryOperator(
            dag=dag,
            config=self.config,
            task_id=head_task_id,
            table=table,
            dataset=self.staging,
            run_name=f'{dag.dag_id}.import-{table.table_name}',
            offset_minutes=60,
            priority_weight=table.priority_weight,
            do_xcom_push=True,
            retries=table.retries,
            on_failure_callback=alerts.setup_callback(is_opsgenie_is_enabled=table.opsgenie.get('enabled'),
                                                      opsgenie_priority=table.opsgenie.get('priority')),
            retry_delay=timedelta(minutes=15),
            execution_timeout=timedelta(hours=3),
        )
        copy_staging_table_to_raw_layer = self._create_copy_to_staging_task(
            dag,
            dataset,
            export_read_replica.is_enabled,
            project_id,
            table,
        )
        export_read_replica >> copy_staging_table_to_raw_layer
        if table.keep_last_import_only:
            copy_staging_table_to_raw_layer >> self._delete_duplicates(dag, dataset, project_id, table)

        return Pipeline(export_read_replica, copy_staging_table_to_raw_layer)

    def _delete_duplicates(self, dag: DAG, dataset: str, project_id: str, table: TableSettings) -> BigQueryOperator:
        return BigQueryOperator(
            dag=dag,
            task_id=create_task_id(f'del-dup-{table.table_name}'),
            bigquery_conn_id=self.bigquery_flat,
            params={
                'project_id': project_id,
                'dataset': dataset,
                'table_name': table.table_name,
                'pk_columns': table.pk_columns,
                'extra_partition_columns': self.get_extra_partition_columns(table)
            },
            sql=self.read_query_file('cleanup/delete_duplicates.sql'),
            use_legacy_sql=False,
            execution_timeout=timedelta(minutes=60),
            on_failure_callback=alerts.setup_callback(),
            enabled=table.cleanup_enabled,
            ui_color='#eb5946',
            ui_fgcolor='#FFF',
            pool=self.bigquery_flat_pool,
        )

    def _create_copy_to_staging_task(self,
                                     dag: DAG,
                                     dataset: str,
                                     is_enabled: bool,
                                     project_id: str,
                                     table: TableSettings):
        destination_dataset_table = table.table_name if table.destination_table_name is None else table.destination_table_name
        copy_staging_table_to_raw_layer = BigQueryOperator(
            dag=dag,
            task_id=create_task_id(f'cp-st-{table.table_name}'),
            bigquery_conn_id=self.bigquery_flat,
            destination_dataset_table=f'{project_id}.{dataset}.{destination_dataset_table}',
            params={
                'project_id': project_id,
                'dataset': self.staging,
                'table_name': table.table_name,
                'time_columns': table.time_columns,
                'except_columns': self.except_columns(table, self.columns_to_remove()),
            },
            sql=self.read_query_file('import/copy_staging_table.sql'),
            time_partitioning=self.get_time_partitioning_column(table),
            cluster_fields=self.get_cluster_fields(table),
            use_legacy_sql=False,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND',
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
            schema_update_options=("ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"),
            enabled=table.job_config.get('copy_staging_to_raw_layer', {}).get('enabled', True) and is_enabled,
            pool=self.bigquery_flat_pool,
        )
        return copy_staging_table_to_raw_layer

    def import_gcp_rds_tables(self, dag: DAG, dataset: str, head_task_id: str, project_id: str,
                              table: TableSettings) -> Pipeline:

        gcp_table_identifier = table.generate_gcp_dataproc_cluster_name()
        create_cluster = DataprocCreateClusterOperator(
            dag=dag,
            task_id=create_task_id(f'create-cluster-{table.table_name}'),
            config=self.config,
            cluster_name=f"import-{gcp_table_identifier}",
            table=table,
            retries=table.retries,
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
        )

        job_name = f'{dag.dag_id}-import-{table.table_name}'.replace('_', '-')
        export_read_replica = DataprocExportReadReplicaToBigQueryOperator(
            dag=dag,
            config=self.config,
            task_id=head_task_id,
            cluster_name=f"import-{gcp_table_identifier}",
            job_name=job_name,
            table=table,
            dataset=self.staging,
            offset_minutes=60,
            priority_weight=table.priority_weight,
            do_xcom_push=True,
            retries=table.retries,
            on_failure_callback=alerts.setup_callback(),
            retry_delay=timedelta(minutes=15),
            execution_timeout=timedelta(hours=3),
        )

        copy_staging_table_to_raw_layer = self._create_copy_to_staging_task(
            dag,
            dataset,
            export_read_replica.is_enabled,
            project_id,
            table,
        )

        delete_cluster = DataprocDeleteClusterOperator(
            dag=dag,
            task_id=create_task_id(f'delete-cluster-{table.table_name}'),
            config=self.config,
            cluster_name=f"import-{gcp_table_identifier}",
            trigger_rule=TriggerRule.ALL_DONE,
            table=table,
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
        )

        create_cluster >> export_read_replica >> [delete_cluster, copy_staging_table_to_raw_layer]
        if table.keep_last_import_only:
            copy_staging_table_to_raw_layer >> self._delete_duplicates(dag, dataset, project_id, table)

        return Pipeline(create_cluster, copy_staging_table_to_raw_layer)

    def import_redshift_s3_tables(self, dag: DAG, dataset: str, head_task_id: str, project_id: str,
                                  table: TableSettings) -> Pipeline:
        pandora_table_name = table.redshift_s3['table_name']
        aws_bucket_name = table.database.redshift_s3['bucket_name']
        delete_files = table.redshift_s3['delete_files']
        aws_prefix = f'parquet/{pandora_table_name}'

        gcs_bucket = self.config.get('bigquery').get('bucket_name')
        gcs_prefix = f'{table.source_format}_import/{table.table_name}/{{{{ ts_nodash }}}}'

        object_conditions = {
            "includePrefixes": [
                f"{aws_prefix}/",
            ]
        }
        transfer_options = {
            "overwriteObjectsAlreadyExistingInSink": True,
            "deleteObjectsFromSourceAfterTransfer": delete_files,
        }

        cont_task_id = create_task_id(f'cont-import-{table.table_name}')
        skip_task_id = create_task_id(f'skip-import-{table.table_name}')
        s3_check_file_task_id = create_task_id(f"s3-check-file-{table.table_name}")

        cont_import = DummyOperator(
            dag=dag,
            task_id=cont_task_id
        )

        skip_import = DummyOperator(
            dag=dag,
            task_id=skip_task_id
        )

        end_import = DummyOperator(
            dag=dag,
            task_id=create_task_id(f"end-import-{table.table_name}"),
            trigger_rule="none_failed"
        )

        s3_check_object_operator = S3CheckObjectExistsOperator(
            dag=dag,
            task_id=s3_check_file_task_id,
            bucket_name=aws_bucket_name,
            object_name=f"{aws_prefix}/*.parquet",
            s3_conn_id='aws_pandora_import',
            wildcard_match=True,
            do_xcom_push=True,
            on_failure_callback=alerts.setup_callback(),
        )

        def branch(**kwargs):
            file_exists = kwargs['task_instance'].xcom_pull(
                task_ids=s3_check_file_task_id,
                key=S3_OBJECT_EXISTS,
            )
            if file_exists:
                return cont_task_id
            else:
                # The path to be skipped needs to be returned
                return skip_task_id

        branch_operator = BranchPythonOperator(
            dag=dag,
            task_id=create_task_id(f'branch-{table.table_name}'),
            python_callable=branch,
            provide_context=True,
            on_failure_callback=alerts.setup_callback(),
        )

        s3_to_gcs_operator = S3ToGoogleCloudStorageTransferOperator(
            dag=dag,
            task_id=create_task_id(f"s3-to-gcs-{table.table_name}"),
            s3_bucket=aws_bucket_name,
            gcs_bucket=gcs_bucket,
            aws_conn_id='aws_pandora_import',
            transfer_options=transfer_options,
            object_conditions=object_conditions,
            timeout=600,
            execution_timeout=timedelta(minutes=15),
            pool='export_read_replica_misc',
            on_failure_callback=alerts.setup_callback(),
        )

        gcs_move_operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            dag=dag,
            task_id=create_task_id(f"move-{table.table_name}"),
            source_bucket=gcs_bucket,
            source_object=f"{aws_prefix}/*.parquet",
            destination_bucket=gcs_bucket,
            destination_object=f"{gcs_prefix}/",
            move_object=True,
            on_failure_callback=alerts.setup_callback(),
        )

        gcs_to_bigquery = GoogleCloudStorageToBigQueryOperator(
            dag=dag,
            task_id=head_task_id,
            bucket=gcs_bucket,
            source_objects=[
                f"{gcs_prefix}/*.parquet"
            ],
            destination_project_dataset_table=f'{project_id}.{self.staging}.{table.table_name}_{{{{ ts_nodash }}}}',
            source_format=table.source_format,
            skip_leading_rows=1,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            execution_timeout=timedelta(minutes=30),
            on_failure_callback=alerts.setup_callback(),
            max_bad_records=0,
            pool='export_read_replica_misc'
        )

        copy_staging_table_to_raw_layer = BigQueryOperator(
            dag=dag,
            task_id=f'cp-st-{table.table_name}',
            bigquery_conn_id=self.bigquery_flat,
            destination_dataset_table=f'{project_id}.{dataset}.{table.table_name}',
            params={
                'project_id': project_id,
                'dataset': self.staging,
                'table_name': table.table_name,
                'extra_columns': table.redshift_s3['extra_columns'],
                'hidden_columns': table.redshift_s3['hidden_columns'],
            },
            sql=self.read_query_file('import/copy_staging_pandora.sql'),
            time_partitioning=self.get_time_partitioning_column(table),
            cluster_fields=self.get_cluster_fields(table),
            use_legacy_sql=False,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition=table.write_disposition,
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
            enabled=True,
            pool=self.bigquery_flat_pool,
        )

        s3_file_sanity_check = BigQueryCheckOperator(
            dag=dag,
            task_id=create_task_id(f'sc-s3-{table.table_name}'),
            bigquery_conn_id=self.bigquery_flat,
            sql=self.read_query_file('sanity_check/s3_import_check.sql'),
            stop_on_errors=True,
            params={
                'project_id': project_id,
                'dataset': dataset,
                'table_name': table.table_name,
            },
            retries=3,
            on_failure_callback=alerts.setup_callback(),
            retry_delay=timedelta(minutes=5),
            pool=self.bigquery_flat_pool,
        )
        end_import >> s3_file_sanity_check

        s3_check_object_operator >> branch_operator
        branch_operator >> cont_import
        branch_operator >> skip_import >> end_import
        cont_import >> s3_to_gcs_operator >> gcs_move_operator >> gcs_to_bigquery >> copy_staging_table_to_raw_layer >> end_import

        return Pipeline(s3_check_object_operator, end_import)

    def import_s3_tables(self, dag: DAG, dataset: str, head_task_id: str, project_id: str,
                         table: TableSettings) -> Pipeline:
        s3_config = table.s3
        aws_prefix = s3_config['prefix']
        aws_bucket_name = s3_config['bucket']

        gcs_bucket = self.config.get('bigquery').get('bucket_name')
        gcs_prefix = '{{ ts_nodash }}'
        gcs_source_object = f'{gcs_prefix}/{aws_prefix}*'

        s3_to_gcs_operator = S3ToGoogleCloudStorageOperator(
            dag=dag,
            task_id=create_task_id(f's3-to-gcs-{table.table_name}'),
            aws_conn_id=s3_config['aws_conn'],
            bucket=aws_bucket_name,
            prefix=aws_prefix,
            dest_gcs_conn_id='google_cloud_default',
            dest_gcs=f'gs://{gcs_bucket}/{gcs_prefix}/',
            replace=True,
            execution_timeout=timedelta(minutes=30),
            on_failure_callback=alerts.setup_callback(),
            executor_config={
                'KubernetesExecutor': {
                    'request_cpu': "200m",
                    'limit_cpu': "400m",
                    'request_memory': "4000Mi",
                    'limit_memory': "4000Mi",
                }
            },
            pool='export_read_replica_misc',
        )

        schema_fields = s3_config.get('schema_fields')
        autodetect = not schema_fields
        gcs_to_bigquery = GoogleCloudStorageToBigQueryOperator(
            dag=dag,
            task_id=head_task_id,
            bucket=gcs_bucket,
            source_objects=[gcs_source_object],
            schema_fields=schema_fields,
            destination_project_dataset_table=f'{self.staging}.{table.table_name}_{{{{ ts_nodash }}}}',
            source_format=table.source_format,
            skip_leading_rows=1,
            allow_quoted_newlines=s3_config.get('allow_quoted_newlines', True),
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            execution_timeout=timedelta(minutes=30),
            on_failure_callback=alerts.setup_callback(),
            max_bad_records=0,
            autodetect=autodetect,
            pool='export_read_replica_misc',
        )

        copy_staging_table_to_raw_layer = BigQueryOperator(
            dag=dag,
            task_id=create_task_id(f'cp-staging-table-rl-{table.table_name}'),
            bigquery_conn_id=self.bigquery_flat,
            destination_dataset_table=f'{project_id}.{dataset}.{table.table_name}',
            params={
                'project_id': project_id,
                'dataset': self.staging,
                'table_name': table.table_name,
                'extra_columns': table.s3.get('extra_columns', None),
            },
            sql=self.read_query_file('import/copy_staging_s3.sql'),
            time_partitioning=self.get_time_partitioning_column(table),
            cluster_fields=self.get_cluster_fields(table),
            use_legacy_sql=False,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND',
            schema_update_options=("ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"),
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
            enabled=True,
            pool=self.bigquery_flat_pool,
        )

        s3_to_gcs_operator >> gcs_to_bigquery >> copy_staging_table_to_raw_layer

        return Pipeline(s3_to_gcs_operator, copy_staging_table_to_raw_layer)

    def import_redshift_tables(self, dag: DAG, dataset: str, head_task_id: str, project_id: str,
                               table: TableSettings) -> Pipeline:
        rs_config = table.redshift
        s3_config = table.s3

        s3_prefix = f'{s3_config["prefix"]}/{{{{ ts_nodash }}}}'
        unload_options = rs_config.get('unload_options')
        unload_options = [uo.upper().strip() for uo in unload_options]
        execution_timeout_min = rs_config.get('execution_timeout')

        if table.source_format.lower() == 'parquet':
            include_header = False
            parquet_str = 'FORMAT AS PARQUET'
            if parquet_str not in unload_options:
                unload_options.append(parquet_str)
        else:
            include_header = rs_config.get('include_header')

        redshift_to_s3_operator = RedshiftToS3IamOperator(
            dag=dag,
            task_id=create_task_id(f'redshift-to-s3-{table.table_name}'),
            s3_bucket=s3_config['bucket'],
            s3_prefix=s3_prefix,
            schema=rs_config['schema'],
            table_name=rs_config['table_name'],
            columns=rs_config.get('columns'),
            redshift_conn_id=rs_config['redshift_conn_id'],
            unload_options=unload_options,
            autocommit=True,
            include_header=include_header,
            execution_timeout=timedelta(minutes=execution_timeout_min),
            on_failure_callback=alerts.setup_callback(),
            executor_config={
                'KubernetesExecutor': {
                    'request_cpu': '200m',
                    'limit_cpu': '400m',
                    'request_memory': '4000Mi',
                    'limit_memory': '4000Mi',
                }
            },
            pool='export_read_replica_misc',
        )
        table.s3['prefix'] = s3_prefix + '/' + rs_config['table_name']

        s3_import_pipeline = self.import_s3_tables(dag, dataset, head_task_id, project_id, table)
        redshift_to_s3_operator >> s3_import_pipeline.head

        return Pipeline(redshift_to_s3_operator, s3_import_pipeline.tail)

    def create_sanity_check_operators(self,
                                      project_id,
                                      dag: DAG,
                                      table: TableSettings,
                                      dataset: str) -> BaseOperator:
        operators = []
        created_date_null = BigQueryCheckOperator(
            dag=dag,
            task_id=create_task_id(f'sc-cd-{table.table_name}'),
            bigquery_conn_id=self.bigquery_flat,
            sql=self.read_query_file('sanity_check/created_date_null.sql'),
            stop_on_errors=True,
            params={
                'project_id': project_id,
                'dataset': dataset,
                'table_name': table.table_name,
                'time_partitioning': table.time_partitioning
            },
            retries=3,
            on_failure_callback=alerts.setup_callback(),
            retry_delay=timedelta(minutes=5),
            enabled=table.sanity_check_created_date_null_enabled,
            pool=self.bigquery_flat_pool,
        )
        duplicate_check = BigQueryCheckOperator(
            dag=dag,
            task_id=create_task_id(f'sc-dc-{table.table_name}'),
            bigquery_conn_id=self.bigquery_flat,
            sql=self.read_query_file('sanity_check/duplicate_check.sql'),
            stop_on_errors=True,
            project_id=project_id,
            dataset=self.dl_dataset,
            table_name=table.table_name,
            ignore_if_table_does_not_exist=True,
            params={
                'project_id': project_id,
                'dataset': self.dl_dataset,
                'table_name': table.table_name,
                'time_partitioning': table.time_partitioning,
                'pk_columns': table.pk_columns,
                'hidden_columns': table.hidden_columns,
                'require_partition_filter': table.require_partition_filter,
            },
            retries=3,
            on_failure_callback=alerts.setup_callback(),
            retry_delay=timedelta(minutes=5),
            enabled=table.sanity_check_duplicate_check_enabled,
            pool=self.bigquery_flat_pool,
        )
        operators.extend([created_date_null, duplicate_check])

        start_sanity_check = DummyOperator(
            dag=dag,
            task_id=create_task_id(f'sc-{table.table_name}')
        )
        start_sanity_check >> operators

        return start_sanity_check

    @staticmethod
    def is_execution_date_next_day(**kwargs) -> bool:
        execution_date = kwargs["execution_date"]
        next_execution_date = kwargs["next_execution_date"]

        return next_execution_date.date() != execution_date.date()

    @staticmethod
    def get_cluster_fields(table: TableSettings) -> List:
        cluster_fields = None
        if table.has_created_date_column:
            cluster_fields = table.pk_columns
        return cluster_fields

    @staticmethod
    def get_time_partitioning_column(table: TableSettings) -> Dict[str, str]:
        time_partitioning_column = None
        if table.has_created_date_column:
            time_partitioning_column = {
                'field': table.time_partitioning,
                'type': 'DAY',
            }
        return time_partitioning_column

    @staticmethod
    def except_columns(table, columns_to_remove):
        return ', '.join(f'`{c}`' for c in columns_to_remove + table.time_columns)

    @staticmethod
    def columns_to_remove():
        return ['merge_layer_created_at', 'merge_layer_updated_at']

    @staticmethod
    def get_extra_partition_columns(table):
        extra_columns = []
        if table.partition_by_date or table.keep_last_import_only:
            if table.created_at_column is not None:
                extra_columns.extend([table.created_at_column])
            if table.updated_at_column is not None:
                extra_columns.extend([table.updated_at_column])
        return list(set(extra_columns))

    def create_views(self, project_id, dag, dataset, table: TableSettings, target_project_id=None):
        dataset_dl = self.dl_dataset
        dataset_hl = self.hl_dataset
        ui_color = '#78a0f0'

        if target_project_id is None:
            suffix = ''
            target_project_id = project_id
        else:
            suffix = 'st-'

        partition_by = table.pk_columns.copy()
        if table.filter_import_by_date:
            updated_at_column = table.updated_at_column
            partition_by.append(table.time_partitioning)
        else:
            updated_at_column = table.deduplicate_order_by_column

        create_view_dl = BigQueryOperator(
            dag=dag,
            task_id=create_task_id(f'view-{suffix}{dataset_dl}-{table.table_name}'),
            params={
                'project_id': project_id,
                'target_project_id': target_project_id,
                'dataset': dataset,
                'table_name': table.table_name,
                'time_partitioning': table.time_partitioning,
                'partition_by': partition_by,
                'updated_at_column': updated_at_column,
                'view_dataset': dataset_dl,
                'partition_time_expiration': None,
                'source_table': table.source_table,
                'sharding_column': table.sharding_column,
                'keep_last_import_only_partition_by_pk': table.keep_last_import_only_partition_by_pk,
                'created_at_column': table.created_at_column,
                'hidden_columns': table.hidden_columns
            },
            sql=self.read_query_file(self.get_query_filename(table)),
            use_legacy_sql=False,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
            enabled=self.config.get('bigquery').get('create_views'),
            ui_color=ui_color,
            pool='export_read_replica_misc',
        )

        partition_by_historic = partition_by.copy()
        sql_historic, updated_at_column_historic = self.get_historic_values(table, partition_by_historic,
                                                                            updated_at_column)

        create_view_hl = BigQueryOperator(
            dag=dag,
            task_id=create_task_id(f'view-{suffix}{dataset_hl}-{table.table_name}'),
            params={
                'project_id': project_id,
                'target_project_id': target_project_id,
                'dataset': dataset,
                'table_name': table.table_name,
                'time_partitioning': table.time_partitioning,
                'partition_by': partition_by_historic,
                'updated_at_column': updated_at_column_historic,
                'view_dataset': dataset_hl,
                'partition_time_expiration': None,
                'source_table': table.source_table,
                'sharding_column': table.sharding_column,
                'keep_last_import_only_partition_by_pk': table.keep_last_import_only_partition_by_pk,
                'created_at_column': table.created_at_column,
                'hidden_columns': table.hidden_columns
            },
            sql=self.read_query_file(sql_historic),
            use_legacy_sql=False,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
            enabled=self.config.get('bigquery').get('create_views'),
            ui_color=ui_color,
            pool='export_read_replica_misc',
        )

        if table.source in [DataSource.BigQuery, DataSource.GoogleAnalytics,
                            DataSource.Firebase] and table.authorize_view:
            create_view_dl >> self.create_authorized_views(dag, table, suffix, target_project_id, dataset_dl)
            create_view_hl >> self.create_authorized_views(dag, table, suffix, target_project_id, dataset_hl)

        return [create_view_dl, create_view_hl]

    def create_authorized_views(self, dag, table: TableSettings, suffix, project_id, dataset):

        sources = self.get_source_details(table.source_table)
        source_dataset = sources["dataset"]
        source_project_id = sources["project_id"]

        authorize_view = BigQueryAuthorizeViewOperator(
            dag=dag,
            task_id=create_task_id(f"auth-{suffix}{dataset}-{table.table_name}"),
            source_dataset=source_dataset,
            view_dataset=dataset,
            view_table=table.table_name,
            source_project=source_project_id,
            view_project=project_id,
            on_failure_callback=alerts.setup_callback(),
            execution_timeout=timedelta(minutes=5),
            pool='export_read_replica_misc',
        )
        return authorize_view

    def get_historic_values(self, table: TableSettings, partition_by_historic, updated_at_column):
        if table.source in [DataSource.RDS, DataSource.CLOUD_SQL] and table.enable_historical_data_in_hl:
            if table.time_partitioning in partition_by_historic:
                partition_by_historic.extend([table.created_at_column, table.updated_at_column])
            else:
                partition_by_historic.extend(
                    [table.time_partitioning, table.created_at_column, table.updated_at_column])
            updated_at_column_historic = table.updated_at_column
            query = 'views/filter_duplicates.sql'
        else:
            query = self.get_query_filename(table)
            updated_at_column_historic = updated_at_column
        return query, updated_at_column_historic

    def get_query_filename(self, table: TableSettings):
        if table.source in [DataSource.RDS, DataSource.CLOUD_SQL, DataSource.DynamoDB, DataSource.API, DataSource.SQS,
                            DataSource.S3, DataSource.Redshift, DataSource.Debezium]:
            if table.keep_last_import_only:
                query = 'views/filter_last_import_only.sql'
            else:
                query = 'views/filter_duplicates.sql'
        elif table.source in [DataSource.RedShift_S3]:
            if table.write_disposition == 'WRITE_APPEND':
                query = 'views/filter_duplicates.sql'
            else:
                query = 'views/redshift_s3.sql'
        elif table.source == DataSource.Firebase:
            query = 'views/firebase.sql'
        elif table.source == DataSource.GoogleAnalytics:
            query = 'views/google_analytics.sql'
        elif table.source == DataSource.BigQuery:
            query = 'views/big_query.sql'
        elif table.source == DataSource.KinesisStream:
            query = 'views/stream_table.sql'
        else:
            raise AirflowException(f'"table.source" parameter missing for table {table.table_name}')
        return query

    def get_source_details(self, source_table):
        sources = source_table.split('.')
        if len(sources) != 3:
            raise AirflowException(f"Invalid 'source.table' parameter {source_table}")
        return {"project_id": sources[0], "dataset": sources[1], "table": sources[2]}
