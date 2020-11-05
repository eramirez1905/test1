from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class NoneCheckOperator(BaseOperator):
    """
    Performs checks against a db. The ``NoneCheckOperator`` is an
    adapted version of the ``airflow.operators.check_operator.CheckOperator``.
    It will do a sanity check by executing a sql query that should return
    nothing/none (no rows).

    If the sql query return records it will raise an AirflowException.

    If you need to have a sanity check that the sql query DO NEEDS to return
    rows, instead of none, please consider using the `CheckOperator`.

    :param sql: the sql to be executed. (templated)
    :type sql: string
    """

    template_fields = (
        "sql",
        "conn_id",
    )
    template_ext = (".sql",)

    @apply_defaults
    def __init__(
        self, sql: str, conn_id: str = "postgres_default", *args, **kwargs
    ) -> None:

        super(NoneCheckOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.sql = sql

    def execute(self, context=None):
        self.log.info("Executing SQL check: %s", self.sql)
        records = self.get_db_hook().get_first(self.sql)
        self.log.info("Record: %s", records)

        if records:
            raise AirflowException("Query returned rows. Sanity check failed!")

        self.log.info("Success.")

    def get_db_hook(self):
        return PostgresHook(postgres_conn_id=self.conn_id)
