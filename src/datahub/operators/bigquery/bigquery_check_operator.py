from airflow import AirflowException
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator as BigQueryCheckOperatorBase
from airflow.utils.decorators import apply_defaults

from datahub.hooks.bigquery_hook import BigQueryHook


class BigQueryCheckOperator(BigQueryCheckOperatorBase):
    """
    Performs checks against BigQuery. The ``BigQueryCheckOperator`` expects
    a sql query that will return a single row. Each value on that
    first row is evaluated using python ``bool`` casting. If any of the
    values return ``False`` the check is failed and errors out.

    Note that Python bool casting evals the following as ``False``:

    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
    the count ``== 0``. You can craft much more complex query that could,
    for instance, check that the table has the same number of rows as
    the source table upstream, or that the count of today's partition is
    greater than yesterday's partition, or that a set of metrics are less
    than 3 standard deviation for the 7 day average.

    This operator can be used as a data quality check in your pipeline, and
    depending on where you put it in your DAG, you have the choice to
    stop the critical path, preventing from
    publishing dubious data, or on the side and receive email alterts
    without stopping the progress of the DAG.

    :param sql: the sql to be executed
    :type sql: string
    :param project_id BigQuery Project id where the table will be checked if it exists.
    :type project_id: str
    :param dataset Dataset in BigQuery where the table will be checked if it exists.
    :type: dataset: str
    :param table_name Table that will be checked if it exists.
    :type: table_name: str
    :param ignore_if_table_does_not_exist If it is desired to ignore the error that table_name does not exists.
    :type: check_if_table_exists: boolean
    :param bigquery_conn_id: reference to the BigQuery database
    :type bigquery_conn_id: string
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
    :type use_legacy_sql: boolean
    :param stop_on_errors: Fails the task (true)
        or send a slack notification (false)
    :type stop_on_errors: boolean
    """

    ui_color = '#843468'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(self,
                 sql,
                 task_id,
                 project_id=None,
                 dataset=None,
                 table_name=None,
                 ignore_if_table_does_not_exist=False,
                 bigquery_conn_id='bigquery_default',
                 use_legacy_sql=False,
                 stop_on_errors=True,
                 enabled=True,
                 delegate_to=None,
                 *args, **kwargs):
        super(BigQueryCheckOperator, self).__init__(sql=sql,
                                                    task_id=task_id,
                                                    *args, **kwargs)
        self.bigquery_conn_id = bigquery_conn_id
        self.sql = sql
        self.use_legacy_sql = use_legacy_sql
        self.stop_on_errors = stop_on_errors
        self.is_enabled = enabled
        self.delegate_to = delegate_to
        self.project_id = project_id
        self.dataset: str = dataset
        self.table_name = table_name
        self.ignore_if_table_does_not_exist = ignore_if_table_does_not_exist

        if not self.is_enabled:
            self.ui_color = '#000'
            self.ui_fgcolor = '#FFF'

    def execute(self, context=None):
        if not self.is_enabled:
            self.log.info('Task disabled, do not run')
            return

        if self.ignore_if_table_does_not_exist and not self.__is_table_exists():
            self.log.info('Table does not exist. Skipping because we ignore the existence of the table.')
            return

        try:
            super().execute(context)
        except AirflowException as e:
            if self.stop_on_errors:
                raise e
            else:
                self.on_failure_callback(context)

    def __is_table_exists(self):
        if self.project_id is None or self.dataset is None or self.table_name is None:
            raise AirflowException(f'Parameters in BigQueryCheckOperator are set to None when check_if_table_exists is set to True.')

        hook = BigQueryHook(
            bigquery_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to,
        )

        return hook.table_exists(project_id=self.project_id, dataset_id=self.dataset, table_id=self.table_name)
