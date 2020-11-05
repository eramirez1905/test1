import os
from datetime import timedelta
from typing import Dict, Callable

from airflow import DAG, AirflowException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.utils.trigger_rule import TriggerRule

from configuration import config as airflow_config
from datahub.common import alerts
from datahub.common.helpers import CuratedDataTable, CuratedDateTableType, create_task_id
from datahub.curated_data.metadata_execution_table import MetadataExecutionTable
from datahub.dwh_import_read_replica import Pipeline, DwhImportReadReplica
from datahub.operators.bigquery.big_query_to_big_query_operator import BigQueryToBigQueryOperator
from datahub.operators.bigquery.bigquery_authorized_view_operator import BigQueryAuthorizeViewOperator
from datahub.operators.bigquery.bigquery_check_metadata_execution_operator import BigQueryCheckMetadataExecutionOperator
from datahub.operators.bigquery.bigquery_check_operator import BigQueryCheckOperator
from datahub.operators.bigquery.bigquery_create_empty_dataset_operator import BigQueryCreateEmptyDatasetOperator
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from datahub.operators.bigquery.bigquery_policy_tags_operator import BigQueryPolicyTagsOperator
from datahub.operators.bigquery.bigquery_table_patch_operator import BigQueryTablePatchOperator
from datahub.operators.bigquery.bigquery_update_metadata_execution_operator import BigQueryUpdateMetadataExecutionOperator
from datahub.operators.bigquery.curated_data_bigquery_operator import CuratedDataBigQueryOperator


class ProcessCuratedData:
    def __init__(self,
                 dag: DAG,
                 project_id: str,
                 dataset_id: str,
                 config: dict,
                 policy_tags: list,
                 entities: dict,
                 dwh_import: DwhImportReadReplica,
                 create_daily_tasks: bool = False,
                 backfill: bool = False,
                 pool_name_misc='curated_data_misc',
                 pool_name_queries='curated_data_queries'):
        self.tables: Dict[str, CuratedDataTable] = {}
        self.dag = dag
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.create_daily_tasks = create_daily_tasks
        self.backfill = backfill
        self.config = config
        self.policy_tags = policy_tags
        self.entities = entities
        self.curated_data_filtered_dataset = airflow_config.get('bigquery').get('dataset').get('curated_data_filtered')
        self.curated_data_shared_dataset = airflow_config.get('bigquery').get('dataset').get('curated_data_shared')
        self.staging_dataset = airflow_config.get('bigquery').get('dataset').get('staging')
        self.raw_dataset = airflow_config.get('bigquery').get('dataset').get('raw')
        self.rl_dataset = airflow_config.get('bigquery').get('dataset').get('rl', 'rl')
        self.acl_dataset = airflow_config.get('bigquery').get('dataset').get('acl')
        self.pool_name_misc = pool_name_misc
        self.pool_name_queries = pool_name_queries

        self._populate_tables()
        self.dwh_import = dwh_import

    @property
    def metadata_execution_table(self):
        return MetadataExecutionTable()

    def __read_query_file(self, filename):
        operator_path = os.path.dirname(os.path.realpath(__file__))
        with open(f'{operator_path}/sql/views/{filename}') as f:
            return f.read()

    def _populate_tables(self):
        for source, params in self.config.items():
            for table_params in params.get('tables', []):
                table = CuratedDataTable(
                    source,
                    table_params,
                    dataset=self.dataset_id,
                    ui_color=params.get('ui_color'),
                    default_bigquery_conn_id=params.get('bigquery_conn_id', {}).get("tables", 'bigquery_default'),
                )
                self._add_operator(table=table)
            for table_params in params.get('reports', []):
                table_params['type'] = 'report'
                table_params['create_shared_view'] = False
                table_params['filter_by_entity'] = False
                table_params['is_stream'] = False
                table_params['columns'] = []
                table = CuratedDataTable(
                    source,
                    table_params,
                    dataset=self.rl_dataset,
                    ui_color='#de870b',
                    prefix_path='/reports',
                    default_bigquery_conn_id=params.get('bigquery_conn_id', {}).get("reports", 'bigquery_default'),
                )
                self._add_operator(table=table)

    def create_operator(self, table: CuratedDataTable) -> Pipeline:
        if table.type in [CuratedDateTableType.Data, CuratedDateTableType.Report]:
            if table.is_stream:
                pipeline = self.process_stream_table_partitions_pipeline(table)
            else:
                task_id_prefix = f'create-table' if table.type == CuratedDateTableType.Data else f'create-report'
                create_curated_data_table_task = CuratedDataBigQueryOperator(
                    dag=self.dag,
                    task_id=f'{task_id_prefix}-{table.dataset}-{table.name}',
                    bigquery_conn_id=table.bigquery_conn_id,
                    sql=table.sql_filename,
                    use_legacy_sql=False,
                    params={
                        'backfill': self.backfill,
                        'entities': self.entities,
                        'table_name': table.name,
                        'project_id': self.project_id,
                        **table.template_params,
                    },
                    execution_timeout=timedelta(minutes=table.execution_timeout_minutes),
                    on_failure_callback=alerts.setup_callback(opsgenie_priority='P1'),
                    ui_color=table.ui_color,
                    enabled=not self.backfill,
                    pool=table.pool_name or self.pool_name_queries,
                )
                pipeline = Pipeline(create_curated_data_table_task, create_curated_data_table_task)

            update_metadata_execution_operator = BigQueryUpdateMetadataExecutionOperator(
                dag=self.dag,
                task_id=f'metadata-{table.name}',
                bigquery_conn_id='bigquery_default',
                project_id=self.project_id,
                dataset_id=table.dataset,
                table_name=table.name,
                metadata_execution_table=self.metadata_execution_table,
                is_shared=table.create_shared_view,
                execution_timeout=timedelta(minutes=15),
                on_failure_callback=alerts.setup_callback(),
                pool=self.pool_name_misc,
            )
            pipeline.tail >> update_metadata_execution_operator

            if table.type == CuratedDateTableType.Data:
                policy_tags_operator = BigQueryPolicyTagsOperator(
                    dag=self.dag,
                    task_id=f'policy-tags-{table.name}',
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    table_id=table.name,
                    policy_tags=self.policy_tags,
                    columns=table.policy_tags,
                    on_failure_callback=alerts.setup_callback(),
                )

                pipeline.tail >> policy_tags_operator

        elif table.type == CuratedDateTableType.SanityCheck:
            create_curated_data_table_task = BigQueryCheckOperator(
                task_id=f"sc-{table.name}",
                bigquery_conn_id=table.bigquery_conn_id,
                sql=f"/sanity_checks/{table.name}.sql",
                params={
                    'project_id': self.project_id,
                },
                on_failure_callback=alerts.setup_callback(),
                retries=1,
                pool=self.pool_name_misc,
            )
            pipeline = Pipeline(create_curated_data_table_task, create_curated_data_table_task)
        else:
            raise AirflowException(f'Table type "{table.type}" is invalid for table {table.name}')

        if len(table.sanity_checks) > 0:
            end_sanity_check = DummyOperator(
                dag=self.dag,
                task_id=f"end-sc-{table.dataset}-{table.name}"
            )

            for sanity_check in table.sanity_checks:
                if sanity_check.is_blocking:
                    on_failure_callback: Callable = alerts.setup_callback(opsgenie_priority='P1')
                else:
                    on_failure_callback: Callable = alerts.setup_callback()

                sanity_check_task = BigQueryCheckOperator(
                    task_id=f"sc-{sanity_check.name}",
                    bigquery_conn_id=table.bigquery_conn_id,
                    sql=f"/sanity_checks/{sanity_check.name}.sql",
                    params={
                        'project_id': self.project_id,
                    },
                    on_failure_callback=on_failure_callback,
                    retries=1,
                    pool=self.pool_name_misc,
                )
                if sanity_check.is_blocking:
                    pipeline.tail >> sanity_check_task >> end_sanity_check
                else:
                    pipeline.tail >> sanity_check_task
                    pipeline.tail >> end_sanity_check

            pipeline = Pipeline(pipeline.head, end_sanity_check)

        return pipeline

    def create_backfill_operator(self, table: CuratedDataTable) -> Pipeline:
        return self.backfill_table_pipeline(table) if table.is_stream else None

    def create_daily_tasks_operators(self, table: CuratedDataTable):

        if table.require_partition_filter or table.create_shared_view:
            start_daily_task = DummyOperator(
                dag=self.dag,
                task_id=f"start-daily-tasks-{table.name}"
            )

            end_daily_task = DummyOperator(
                dag=self.dag,
                task_id=f"end-daily-tasks-{table.name}"
            )

            if table.require_partition_filter:
                if table.time_partitioning:
                    require_partition_filter_task = BigQueryTablePatchOperator(
                        task_id=f'set-partition-filter-{table.name}',
                        dataset_id=self.dataset_id,
                        table_id=table.name,
                        require_partition_filter=table.require_partition_filter,
                        pool=self.pool_name_misc,
                    )
                    start_daily_task >> require_partition_filter_task >> end_daily_task
                else:
                    raise AirflowException(f'Cannot set required partition filter, {table.name} has undefined time partitioning.')

            if table.create_shared_view:
                create_curated_data_filtered_view_task = BigQueryOperator(
                    dag=self.dag,
                    # to put hash for task_id at end, when length greater than 63 characters
                    task_id=create_task_id(f"create-{self.curated_data_filtered_dataset}-view-{table.name}"),
                    bigquery_conn_id='bigquery_default',
                    sql=self.__read_query_file('curated_data_filtered_template.sql'),
                    use_legacy_sql=False,
                    on_failure_callback=alerts.setup_callback(),
                    execution_timeout=timedelta(minutes=5),
                    params={
                        'project_id': self.project_id,
                        'dataset_id': self.dataset_id,
                        'curated_data_filtered': self.curated_data_filtered_dataset,
                        'table': table,
                        'acl_dataset': self.acl_dataset,
                    },
                    priority='INTERACTIVE',
                    pool=self.pool_name_misc,
                )
                create_curated_data_shared_view_task = BigQueryOperator(
                    dag=self.dag,
                    task_id=create_task_id(f"create-{self.curated_data_shared_dataset}-view-{table.name}"),
                    bigquery_conn_id='bigquery_default',
                    sql=self.__read_query_file('curated_data_shared_template.sql'),
                    use_legacy_sql=False,
                    on_failure_callback=alerts.setup_callback(),
                    execution_timeout=timedelta(minutes=5),
                    params={
                        'project_id': self.project_id,
                        'curated_data_filtered': self.curated_data_filtered_dataset,
                        'curated_data_shared': self.curated_data_shared_dataset,
                        'table': table,
                    },
                    priority='INTERACTIVE',
                    pool=self.pool_name_misc,
                )
                authorize_view_curated_data_shared = BigQueryAuthorizeViewOperator(
                    task_id=create_task_id(f"auth-view-{self.curated_data_shared_dataset}-{table.name}"),
                    bigquery_conn_id='bigquery_default',
                    source_project=self.project_id,
                    view_project=self.project_id,
                    source_dataset=self.curated_data_filtered_dataset,
                    view_dataset=self.curated_data_shared_dataset,
                    view_table=table.name,
                    on_failure_callback=alerts.setup_callback(),
                    execution_timeout=timedelta(minutes=5),
                    pool=self.pool_name_misc,
                )
                authorize_view_curated_data_filtered = BigQueryAuthorizeViewOperator(
                    task_id=create_task_id(f"auth-view-{self.curated_data_filtered_dataset}-{table.name}"),
                    bigquery_conn_id='bigquery_default',
                    source_project=self.project_id,
                    view_project=self.project_id,
                    source_dataset=self.dataset_id,
                    view_dataset=self.curated_data_filtered_dataset,
                    view_table=table.name,
                    on_failure_callback=alerts.setup_callback(),
                    execution_timeout=timedelta(minutes=5),
                    pool=self.pool_name_misc,
                )
                authorize_view_acl = BigQueryAuthorizeViewOperator(
                    task_id=create_task_id(f"auth-view-acl-{table.name}"),
                    bigquery_conn_id='bigquery_default',
                    source_project=self.project_id,
                    view_project=self.project_id,
                    source_dataset=self.acl_dataset,
                    view_dataset=self.curated_data_filtered_dataset,
                    view_table=table.name,
                    on_failure_callback=alerts.setup_callback(),
                    execution_timeout=timedelta(minutes=5),
                    pool=self.pool_name_misc,
                )
                # Create views
                start_daily_task >> create_curated_data_filtered_view_task >> create_curated_data_shared_view_task
                # Authorize views
                create_curated_data_filtered_view_task >> authorize_view_acl
                create_curated_data_filtered_view_task >> authorize_view_curated_data_filtered
                create_curated_data_shared_view_task >> authorize_view_curated_data_shared
                [authorize_view_curated_data_shared, authorize_view_acl, authorize_view_curated_data_filtered] >> end_daily_task

            return Pipeline(start_daily_task, end_daily_task)
        else:
            raise AirflowException(f'Create daily tasks called for table where partition filter is not required or table is not shared: {table.name}')

    def process_stream_table_partitions_pipeline(self, table: CuratedDataTable) -> Pipeline:
        staging_table_name = f"{self.staging_dataset}.{{{{ dag.dag_id.replace('-', '_') }}}}_stream_table_{{{{ params.table_name }}}}_{{{{ ts_nodash }}}}"
        labels = {
            'dag_id': "{{ dag.dag_id }}",
            'task_id': "{{ task.task_id }}",
        }
        partition_from = '{{ macros.ds_format(macros.ds_add(next_ds, -1), "%Y-%m-%d", "%Y%m%d") }}'
        partition_to = '{{ next_ds_nodash }}'

        staging_curated_data_table_task = CuratedDataBigQueryOperator(
            task_id=f"create-staging-{table.dataset}_{table.name}",
            bigquery_conn_id=table.bigquery_conn_id,
            sql=table.sql_filename,
            destination_dataset_table=f"{self.project_id}.{staging_table_name}",
            time_partitioning=table.time_partitioning,
            cluster_fields=table.cluster_fields,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            use_legacy_sql=False,
            execution_timeout=timedelta(minutes=table.execution_timeout_minutes),
            params={
                'backfill': self.backfill,
                'entities': self.entities,
                'table_name': table.name,
                'project_id': self.project_id,
                **table.template_params,
            },
            on_failure_callback=alerts.setup_callback(),
            ui_color=table.ui_color,
            pool=table.pool_name or self.pool_name_queries,
        )
        copy_partition_tasks = {}
        tasks = [
            {
                'task_id': 'current',
                'partition': partition_from
            },
            {
                'task_id': 'next',
                'partition': partition_to
            }
        ]
        for task in tasks:
            task_id = task.get('task_id')
            partition_name = task.get('partition')
            copy_ds = BigQueryToBigQueryOperator(
                task_id=f'{task_id}-{table.dataset}-{table.name}',
                bigquery_conn_id=table.bigquery_conn_id,
                source_project_dataset_tables=f'{self.project_id}.{staging_table_name}${partition_name}',
                destination_project_dataset_table=f'{self.project_id}.{table.dataset}.{table.name}${partition_name}',
                params={
                    'backfill': self.backfill,
                    'entities': self.entities,
                    'table_name': table.name
                },
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE',
                on_failure_callback=alerts.setup_callback(),
                labels=labels,
                pool=table.pool_name or self.pool_name_queries,
            )
            copy_partition_tasks[task_id] = copy_ds

        join_copy_staging_table = DummyOperator(
            task_id=f'join_{table.dataset}-{table.name}',
            trigger_rule=TriggerRule.NONE_FAILED,
        )

        staging_curated_data_table_task >> [copy_partition_tasks['current'],
                                            copy_partition_tasks['next']] >> join_copy_staging_table

        pipeline = Pipeline(staging_curated_data_table_task, join_copy_staging_table)

        return pipeline

    def backfill_table_pipeline(self, table: CuratedDataTable) -> Pipeline:
        staging_curated_data_table_task = CuratedDataBigQueryOperator(
            task_id=f"backfill-{table.dataset}_{table.name}",
            bigquery_conn_id=table.bigquery_conn_id,
            sql=table.sql_filename,
            destination_dataset_table=f"{self.project_id}.{table.dataset}.{table.name}",
            time_partitioning=table.time_partitioning,
            cluster_fields=table.cluster_fields,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            use_legacy_sql=False,
            params={
                'backfill': self.backfill,
                'entities': self.entities,
                'project_id': self.project_id,
                'table_name': table.name,
                **table.template_params,
            },
            execution_timeout=timedelta(hours=3),
            retries=1,
            on_failure_callback=alerts.setup_callback(),
            ui_color=table.ui_color,
            pool=table.pool_name or self.pool_name_queries,
        )

        pipeline = Pipeline(staging_curated_data_table_task, staging_curated_data_table_task)

        return pipeline

    def _add_operator(self, table: CuratedDataTable):
        if table.name not in self.tables:
            self.tables[table.name] = table

    def render(self) -> Dict[str, Pipeline]:
        start = LatestOnlyOperator(
            dag=self.dag,
            task_id=f'start-{self.dataset_id}'
        )

        create_dataset_task_cl = BigQueryCreateEmptyDatasetOperator(
            task_id=f'create-dataset-{self.dataset_id}',
            dataset_id=self.dataset_id,
            project_id=self.project_id,
            on_failure_callback=alerts.setup_callback(),
            execution_timeout=timedelta(minutes=5),
            pool=self.pool_name_misc,
        )
        create_dataset_task_cl >> start

        if self.create_daily_tasks:
            create_dataset_curated_data_filtered_task = BigQueryCreateEmptyDatasetOperator(
                task_id=f'create-dataset-{self.curated_data_filtered_dataset}',
                dataset_id=self.curated_data_filtered_dataset,
                project_id=self.project_id,
                on_failure_callback=alerts.setup_callback(),
                execution_timeout=timedelta(minutes=5),
                pool=self.pool_name_misc,
            )

            create_dataset_curated_data_shared_task = BigQueryCreateEmptyDatasetOperator(
                task_id=f'create-dataset-{self.curated_data_shared_dataset}',
                dataset_id=self.curated_data_shared_dataset,
                project_id=self.project_id,
                on_failure_callback=alerts.setup_callback(),
                execution_timeout=timedelta(minutes=5),
                pool=self.pool_name_misc,
            )
            start << [create_dataset_curated_data_filtered_task, create_dataset_curated_data_shared_task]

        tasks = self._get_tasks()

        for name, table in self.tables.items():
            if name in tasks:
                if tasks[name] is not None:
                    if not self.backfill:
                        try:
                            start.set_downstream(tasks[name].head)
                        except KeyError as key:
                            raise AirflowException(f'Table {key} does not exist in registered tasks')
                    else:
                        is_independent = True
                        for dependency in table.dependencies:
                            try:
                                if tasks[dependency] is not None:
                                    tasks[dependency].tail >> tasks[name].head
                                    is_independent = False
                            except KeyError as key:
                                raise AirflowException(
                                    f'Table {key} does not exist. table.name={name}, dependency={dependency}')
                        if is_independent:
                            start.set_downstream(tasks[name].head)

                if not self.backfill and not self.create_daily_tasks:
                    if not isinstance(tasks[name], list) and len(tasks[name].head.upstream_list) == 0:
                        try:
                            start.set_downstream(tasks[name].head)
                        except KeyError as key:
                            raise AirflowException(f'Table {key} does not exist in registered tasks')
                    for dependency in table.dependencies:
                        try:
                            tasks[dependency].tail >> tasks[name].head
                        except KeyError as key:
                            raise AirflowException(
                                f'Table {key} does not exist. table.name={name}, dependency={dependency}')
                    for dl_table_name in table.dependencies_dl:
                        dl_table = self.dwh_import.tables[dl_table_name]
                        dl_table_task_id = f'check-mtdt-{dl_table_name}'
                        if self.dag.has_task(dl_table_task_id):
                            check_metadata_execution_operator = self.dag.get_task(dl_table_task_id)
                        else:
                            check_metadata_execution_operator = BigQueryCheckMetadataExecutionOperator(
                                config=airflow_config,
                                dag=self.dag,
                                task_id=dl_table_task_id,
                                bigquery_conn_id='bigquery_default',
                                dataset=self.raw_dataset,
                                table_name=dl_table_name,
                                metadata_execution_table=self.metadata_execution_table,
                                execution_timeout=timedelta(minutes=15),
                                on_failure_callback=alerts.setup_callback(opsgenie_priority='P1'),
                                pool=self.pool_name_misc,
                                enabled=dl_table.is_batch_import,
                            )
                        start >> check_metadata_execution_operator >> tasks[name].head

        return tasks

    def _get_tasks(self):
        if self.create_daily_tasks:
            tasks = {name: self.create_daily_tasks_operators(table) for (name, table) in self.tables.items() if
                     table.require_partition_filter or table.create_shared_view}
        else:
            function_name = self.create_backfill_operator if self.backfill else self.create_operator
            tasks = {name: function_name(table) for (name, table) in self.tables.items()}
        return tasks
