from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from datahub.common import alerts
from datahub.common.helpers import DataSource, TableSettings, create_task_id
from datahub.dwh_import_read_replica import DwhImportReadReplica
from datahub.dwh_import_read_replica import Pipeline
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from datahub.operators.bigquery.bigquery_update_metadata_execution_operator import BigQueryUpdateMetadataExecutionOperator
from datahub.operators.databricks.databricks_export_read_replica_to_bigquery_operator import \
    DatabricksExportReadReplicaToBigQueryOperator
from datahub.operators.dataproc.dataproc_create_cluster_operator import \
    DataprocCreateClusterOperator
from datahub.operators.dataproc.dataproc_delete_cluster_operator import \
    DataprocDeleteClusterOperator
from datahub.operators.dataproc.dataproc_export_read_replica_to_bigquery_operator import \
    DataprocExportReadReplicaToBigQueryOperator


class DwhImportReadReplicaBackfill(DwhImportReadReplica):

    def __init__(self, config: dict):
        super().__init__(config)
        self.config = config
        self.staging_dataset = self.config.get('bigquery').get('dataset').get('staging')
        self.raw_dataset = self.config.get('bigquery').get('dataset').get('raw')
        self.project_id = self.config.get('bigquery').get('project_id')
        self.backfill_from_date = None
        self.backfill_to_date = None
        try:
            from_date = self.config.get("raw_layer_backfill").get('from_date', None)
            self.backfill_from_date = datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S') if from_date else None
            to_date = self.config.get("raw_layer_backfill").get('to_date', None)
            self.backfill_to_date = datetime.strptime(to_date, '%Y-%m-%d %H:%M:%S') if to_date else None
        except ValueError:
            pass

    def import_from_read_replicas(self, dag: DAG, table: TableSettings):
        raise NotImplementedError

    def import_from_read_replicas_backfill(self, dag: DAG):
        for database in self.databases.values():
            database_start = DummyOperator(
                dag=dag,
                task_id=create_task_id(f'start-{database.name}')
            )
            for table in database.tables:
                head_task_id = create_task_id(f'import-{table.table_name}')
                import_pipeline = None
                if table.source == DataSource.RDS:
                    import_pipeline = self.import_rds_tables(dag, self.staging_dataset, head_task_id, self.project_id,
                                                             table)
                elif table.source == DataSource.CLOUD_SQL:
                    import_pipeline = self.import_gcp_rds_tables(dag, self.staging_dataset, head_task_id, self.project_id,
                                                                 table)
                if import_pipeline:
                    database_start >> import_pipeline.head

    def import_rds_tables(self, dag: DAG, dataset: str, head_task_id: str, project_id: str,
                          table: TableSettings) -> Pipeline:

        table_import = DatabricksExportReadReplicaToBigQueryOperator(
            config=self.config,
            dag=dag,
            task_id=head_task_id,
            table=table,
            dataset=dataset,
            run_name=f'{dag.dag_id}.import-{table.table_name}',
            priority_weight=table.priority_weight,
            do_xcom_push=True,
            backfill=True,
            backfill_from_date=self.backfill_from_date,
            backfill_to_date=self.backfill_to_date,
            countries=self.config.get("raw_layer_backfill").get("countries", None),
            regions=self.config.get("raw_layer_backfill").get("regions", None),
            retries=0,
            on_failure_callback=alerts.setup_callback(),
        )

        columns_to_remove = ['merge_layer_created_at', 'merge_layer_updated_at']
        except_columns = ', '.join(f'`{c}`' for c in columns_to_remove + table.time_columns)
        destination_dataset_table = table.table_name if table.destination_table_name is None else table.destination_table_name

        copy_staging_table_to_raw_layer = BigQueryOperator(
            dag=dag,
            task_id=create_task_id(f'cp-st-{table.table_name}'),
            destination_dataset_table=f'{self.project_id}.{self.raw_dataset}.{destination_dataset_table}',
            params={
                'project_id': self.project_id,
                'dataset': dataset,
                'table_name': table.table_name,
                'time_columns': table.time_columns,
                'except_columns': except_columns,
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
            enabled=table.job_config.get('copy_staging_to_raw_layer', {}).get('enabled', True),
        )
        update_metadata_execution_operator = BigQueryUpdateMetadataExecutionOperator(
            dag=dag,
            project_id=self.project_id,
            dataset_id=self.raw_dataset,
            table_name=table.table_name,
            metadata_execution_table=self.metadata_execution_table,
            task_id=create_task_id(f'metadata-{table.table_name}'),
            backfill_to_date=self.backfill_to_date,
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
        )

        table_import >> copy_staging_table_to_raw_layer >> update_metadata_execution_operator

        return Pipeline(table_import, update_metadata_execution_operator)

    def import_gcp_rds_tables(self, dag: DAG, dataset: str, head_task_id: str, project_id: str,
                              table: TableSettings) -> Pipeline:

        gcp_table_identifier = table.generate_gcp_dataproc_cluster_name()
        create_cluster = DataprocCreateClusterOperator(
            dag=dag,
            task_id=create_task_id(f'create-cluster-{table.table_name}'),
            config=self.config,
            backfill=True,
            cluster_name=f"import-{gcp_table_identifier}",
            table=table,
            on_failure_callback=alerts.setup_callback(),
            execution_timeout=timedelta(minutes=15),
        )

        job_name = f'{dag.dag_id}-import-{table.table_name}'.replace('_', '-')
        export_read_replica = DataprocExportReadReplicaToBigQueryOperator(
            dag=dag,
            config=self.config,
            task_id=head_task_id,
            cluster_name=f"import-{gcp_table_identifier}",
            job_name=job_name,
            table=table,
            dataset=dataset,
            offset_minutes=60,
            priority_weight=table.priority_weight,
            do_xcom_push=True,
            backfill=True,
            backfill_from_date=self.backfill_from_date,
            backfill_to_date=self.backfill_to_date,
            countries=self.config.get("raw_layer_backfill").get("countries", None),
            regions=self.config.get("raw_layer_backfill").get("regions", None),
            retries=0,
            on_failure_callback=alerts.setup_callback(),
        )

        copy_staging_table_to_raw_layer = BigQueryOperator(
            dag=dag,
            task_id=create_task_id(f'cp-st-{table.table_name}'),
            destination_dataset_table=f'{project_id}.{self.raw_dataset}.{table.table_name}',
            params={
                'project_id': project_id,
                'dataset': dataset,
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
            enabled=table.job_config.get('copy_staging_to_raw_layer', {}).get('enabled',
                                                                              True) and export_read_replica.is_enabled,
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

        update_metadata_execution_operator = BigQueryUpdateMetadataExecutionOperator(
            dag=dag,
            project_id=self.project_id,
            dataset_id=self.raw_dataset,
            table_name=table.table_name,
            metadata_execution_table=self.metadata_execution_table,
            task_id=create_task_id(f'metadata-{table.table_name}'),
            backfill_to_date=self.backfill_to_date,
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
        )

        create_cluster.set_downstream(export_read_replica)
        export_read_replica.set_downstream(copy_staging_table_to_raw_layer)
        export_read_replica.set_downstream(delete_cluster)
        copy_staging_table_to_raw_layer.set_downstream(update_metadata_execution_operator)

        return Pipeline(create_cluster, update_metadata_execution_operator)
