from copy import deepcopy
from datetime import datetime, timedelta
from enum import Enum, unique
from itertools import zip_longest
from re import sub
import hashlib

from airflow.api.common.experimental import pool
from airflow.exceptions import AirflowException, PoolNotFound


class DatabaseSettings:
    def __init__(self, name: str, params: dict):
        params = deepcopy(params)
        if 'is_regional' not in params:
            raise AirflowException(f'"is_regional" parameter is missing in "{name}" database configuration.')

        if params.get('source') is None:
            raise AirflowException(f'"source" parameter is missing in database {name}')

        self.name = name
        try:
            self.source = DataSource(params.pop('source'))
            self.source_format = params.pop('source_format', 'parquet')
        except ValueError as e:
            raise AirflowException(f"{e} for database '{self.name}', available DataSources: {DataSource.list()}")

        self.opsgenie = {**{'enabled': False, 'priority': 'P3'}, **params.pop('opsgenie', {})}
        self.endpoint = params.pop('endpoint', None)
        self.port = params.pop('port', None)
        self.credentials = params.pop('credentials', {})
        self.create_views = params.pop('create_views', True)
        self.create_maintenance_tasks = params.pop('create_maintenance_tasks', True)
        self.sharding_column = params.pop('sharding_column', 'country_code')
        self.is_regional = params.pop('is_regional')
        self.type: DatabaseType = DatabaseType(params.pop('database_type', 'postgresql'))
        self.job_config = params.pop('job_config', {})
        self.countries = []
        self.redshift_s3 = params.pop('redshift_s3', dict({'bucket_name': None}))
        self.extra_params = params

        self.tables = [TableSettings(self, table_params) for table_params in params.pop('tables', [])]

    def __repr__(self):
        return 'DatabaseSettings(%r)' % self.__dict__


@unique
class DatabaseType(Enum):
    mysql = 'mysql'
    postgresql = 'postgresql'

    def _generate_next_value_(name, start, count, last_values):
        return name

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


class TableSettings:
    def __init__(self, database: DatabaseSettings, params: dict):
        self.database = database
        self.name = params['name']
        self.namespace = params.get('namespace', None)
        try:
            self.source = DataSource(params.get('source', self.database.source))
            self.source_format = params.get('source_format', self.database.source_format)
        except ValueError as e:
            raise AirflowException(f"{e} for table '{self.table_name}', available DataSources: {DataSource.list()}")

        self.opsgenie = {**self.database.opsgenie, **params.get('opsgenie', {})}
        self.filter_import_by_date = params.get('filter_import_by_date', True)
        self.filter_staging_table_by_date = params.get('filter_staging_table_by_date',
                                                       False) or self.filter_import_by_date
        self.keep_last_import_only = params.get('keep_last_import_only', False)
        self.keep_last_import_only_partition_by_pk = params.get('keep_last_import_only_partition_by_pk', False)
        self.source_date_column = params.get('source_date_column')
        self.sharding_column = params.get('sharding_column', self.database.sharding_column)
        self.pk_columns = params.get('pk_columns', [self.sharding_column, 'id'])
        self.partition_by_date = params.get('partition_by_date', self.filter_import_by_date)
        self.time_partitioning = params.get('time_partitioning', 'created_date')
        self.priority_weight = params.get('priority_weight', 100)
        self.partition_time_expiration = params.get('partition_time_expiration', None)
        self.spark_cluster = params.get('spark_cluster', {})
        self.created_at_column = params.get('raw_layer', {}).get('created_at_column', 'created_at')
        self.updated_at_column = params.get('raw_layer', {}).get('updated_at_column', 'updated_at')
        self.time_columns = params.get('time_columns', [])
        self.deduplicate_order_by_column = params.get('deduplicate_order_by_column', '_ingested_at')
        self.retries = params.get('retries', 3)
        self.job_config = params.get('job_config', {})
        self.cleanup_enabled = params.get('cleanup_enabled', True)
        self.write_disposition = params.get('write_disposition', 'WRITE_TRUNCATE')
        self.sanity_check_created_date_null_enabled = params.get('sanity_checks', {}).get('created_date_null', True)
        self.sanity_check_duplicate_check_enabled = params.get('sanity_checks', {}).get('duplicate_check', True)
        self.destination_table_name = params.get('destination_table_name', None)

        # keep_last_import_only expose only the latest snapshot of the source table, so it is mandatory to do a full import
        if self.keep_last_import_only and self.filter_import_by_date:
            raise AirflowException(f"keep_last_import_only and filter_import_by_date are both enabled.\n"
                                   f"{self.table_name} must have filter_import_by_date set to False")
        if self.sanity_check_created_date_null_enabled and not self.has_created_date_column:
            raise AirflowException(f"It's not possible to enable sanity_check_created_date_null_enabled, "
                                   f"{self.table_name} does not have created_date column.\n"
                                   f"If the table is partitioned by DATE, please enable partition_by_date")
        if self.sanity_check_duplicate_check_enabled and not self.has_primary_keys:
            raise AirflowException(f"It's not possible to enable sanity_check_duplicate_check_enabled, "
                                   f"{self.table_name} does not have primary keys set.")

        self.require_partition_filter = params.get('require_partition_filter', False)
        self.source_table = params.get('source_table')
        self.redshift_s3 = {**{
            'table_name': None,
            'extra_columns': [],
            'hidden_columns': [],
            'delete_files': True,
        }, **params.get('redshift_s3', {})}

        self.redshift = params.get('redshift', dict({
            'table_name': None,
            'schema': None,
            'redshift_conn_id': 'redshift_default',
            'columns': ['*'],
            'unload_options': [],
            'include_header': True,
            'execution_timeout': 30,
        }))
        self.s3 = params.get('s3', dict({
            'bucket': None,
            'prefix': '',
            'aws_conn_id': 'aws_default',
            'extra_columns': None,
            'schema_fields': None,
            'allow_quoted_newlines': True,
        }))
        self.file_format = params.get('file_format', 'orc')
        self.pool_name = params.get('pool_name', 'export_read_replica')
        # added to handle tables without historical data
        self.enable_historical_data_in_hl = params.get('enable_historical_data_in_hl', True)
        self.authorize_view = params.get('authorize_view', True)
        self.hidden_columns = params.get('hidden_columns', [])
        self.create_views = params.get('create_views', self.database.create_views)
        self.create_maintenance_tasks = params.get('create_maintenance_tasks', self.database.create_maintenance_tasks)

        if self.source == DataSource.Firebase and self.source_table is None:
            raise AirflowException(f'Parameter source_table is missing for table {self.table_name}')

        if self.source == DataSource.GoogleAnalytics and self.source_table is None:
            raise AirflowException(f'Parameter source_table is missing for table {self.table_name}')

        if self.write_disposition not in ['WRITE_APPEND', 'WRITE_TRUNCATE']:
            raise AirflowException(f'The write disposition {self.write_disposition} is not valid. Possible values are: WRITE_APPEND WRITE_TRUNCATE')

    @property
    def has_created_date_column(self):
        return self.partition_by_date

    @property
    def has_primary_keys(self):
        return len(self.pk_columns) > 0

    @staticmethod
    def generate_staging_table_template(table_name):
        return f"{table_name}_{{{{ execution_date.strftime('%Y%m%d_%H%M%S') }}}}"

    def generate_gcp_dataproc_cluster_name(self):
        # gcp table identifier introduced to comply with gcp cluster naming standards `(?:[a-z](?:[-a-z0-9]{0,
        # 49}[a-z0-9])?)` 22 chars from 50 available are taken by `import_` prefix and datetime suffix if the table
        # name is too long it tries to abbreviate it by taking first few chars of each name segment
        # added further 29 upper limit for table name to handle table with multiple underscores.
        table_name_lower = self.table_name.lower()
        table_name_without_sp_char = sub('[^0-9a-z-]+', '-', table_name_lower)
        cluster_name = table_name_without_sp_char \
            if len(table_name_without_sp_char) < 26 \
            else "-".join([word[:4] for word in table_name_without_sp_char.split('-')])[:26]
        return f"{cluster_name}-{{{{ execution_date.strftime('%Y%m%d-%H%M%S') }}}}"

    @property
    def table_name(self) -> str:
        namespace = "" if self.namespace is None else f"{self.namespace}_"
        return f"{self.database.name}_{namespace}{self.name}"

    @property
    def is_batch_import(self):
        return self.source in [DataSource.RDS, DataSource.CLOUD_SQL, DataSource.S3, DataSource.Redshift]

    def __repr__(self):
        props = {k: (v.name if k == 'database' else v) for k, v in self.__dict__.items()}
        return 'TableSettings(%r)' % props


@unique
class DataSource(Enum):
    RDS = 'RDS'
    CLOUD_SQL = 'CLOUD_SQL'
    Firebase = 'firebase'
    GoogleAnalytics = 'google_analytics'
    BigQuery = 'big_query'
    DynamoDB = 'DynamoDB'
    SQS = 'SQS'
    API = 'API'
    RedShift_S3 = 'RedShift_S3'
    KinesisStream = 'kinesis_stream'
    S3 = 'S3'
    Redshift = 'Redshift'
    Debezium = 'Debezium'

    def _generate_next_value_(name, start, count, last_values):
        return name

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


@unique
class CuratedDateTableType(Enum):
    Data = 'data'
    Report = 'report'
    SanityCheck = 'sanity_check'

    def _generate_next_value_(name, start, count, last_values):
        return name

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


class CuratedDataSanityCheck:
    def __init__(self, name, is_blocking):
        self.name = name
        self.is_blocking = is_blocking


class CuratedDataTable:
    def __init__(self,
                 source: str,
                 params: dict,
                 dataset: str,
                 default_bigquery_conn_id: str = None,
                 ui_color: str = None,
                 prefix_path: str = None):
        self.source = source
        self.dataset = dataset
        self.name = params.get('name')
        self.filter_by_entity = params.get('filter_by_entity', False)
        self.filter_by_country = params.get('filter_by_country', True)
        self.entity_id_column = params.get('entity_id_column', 'entity.id')
        self.columns = params.get('columns', [])
        self.dependencies = params.get('dependencies', [])
        self.dependencies_dl = params.get('dependencies_dl', [])
        self.create_shared_view = params.get('create_shared_view', False)
        self.is_stream = params.get('is_stream', False)
        self.ui_color = ui_color if ui_color is not None else params.get('ui_color')
        self.prefix_path = prefix_path
        self.type = CuratedDateTableType(params.get('type', 'data'))
        self.time_partitioning = params.get('time_partitioning', None)
        self.cluster_fields = params.get('cluster_fields', None)
        self.execution_timeout_minutes = int(params.get('execution_timeout_minutes', 20))
        self.pool_name = params.get('pool_name', None)
        self.bigquery_conn_id = params.get('bigquery_conn_id', default_bigquery_conn_id)
        self.sanity_checks = [CuratedDataSanityCheck(sc.get("name"), sc.get("is_blocking", True)) for sc in
                              params.get('sanity_checks', [])]
        self.policy_tags = params.get("policy_tags", [])
        self.require_partition_filter = params.get("require_partition_filter", False)
        self.sql_template = params.get("sql_template", f"{self.name}.sql")
        self.template_params = params.get("template_params", {})

        if self.require_partition_filter and not self.time_partitioning:
            raise AirflowException(
                f'Parameter require_partition_filter set for table: {self.name} without time_partitioning defined')

        if self.time_partitioning:
            self.time_partitioning = {
                'field': self.time_partitioning,
                'type': 'DAY',
            }

        if not sorted(set(self.columns)) == sorted(self.columns):
            raise AirflowException(f'There are duplicated columns in: {self.name}')

    @property
    def sql_filename(self):
        return self.sql_template if self.prefix_path is None else f"{self.prefix_path}/{self.sql_template}"


def create_pool(pool_name, slots, description):
    try:
        pool.get_pool(pool_name)
    except PoolNotFound:
        pool.create_pool(pool_name, slots, description)


def get_last_dag_run_execution_date(dag=None):
    last_dag_run = dag.get_last_dagrun() if dag else None
    if last_dag_run is None:
        return datetime.now().isoformat()
    else:
        return last_dag_run.execution_date.isoformat()


def get_last_dag_run_next_execution_date(dag=None):
    last_dag_run = dag.get_last_dagrun() if dag else None
    if last_dag_run is None:
        f = datetime.now() + timedelta(days=1)
    else:
        # TODO: Improve the way we get the next execution
        #       ideally it would be last_dag_run.get_dag().following_schedule(last_dag_run.execution_date)
        f = last_dag_run.execution_date + timedelta(days=1)

    return f.isoformat()


def grouper(iterable, n, fill_value=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fill_value)


def create_task_id(task_id: str):
    # To avoid the case where the task ID is > 63 characters
    if len(task_id) < 63:
        return task_id
    else:
        split_label = task_id.split('-')
        hash_8chr = hashlib.md5(split_label[-1].encode('utf-8')).hexdigest()[:8]
        return f"{'-'.join(split_label[:-1])}-{hash_8chr}"
