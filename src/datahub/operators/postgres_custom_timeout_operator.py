import logging

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults


class PostgresCustomTimeoutOperator(BaseOperator):
    """
    Executes sql code in a specific Postgres database with custom timeout

    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql,
            postgres_conn_id='postgres_default', autocommit=False,
            parameters=None,
            statement_timeout=7200,
            *args, **kwargs):
        super(PostgresCustomTimeoutOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.statement_timeout = statement_timeout

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(f"SET statement_timeout TO {int(self.statement_timeout * 1000)};")
        if self.parameters is not None:
            cur.execute(self.sql, self.parameters)
        else:
            cur.execute(self.sql)
        cur.execute(self.sql)
        cur.execute(f"RESET statement_timeout;")
        cur.close()
        conn.commit()
