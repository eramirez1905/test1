"""
Convenience operator to TRUNCATE a table in AWS Redshift.

The same result could be achieve by using a standard PostgresOperator and executing the
TRUNCATE query. Using this RedshiftTruncateOperator instead removes boilerplate from the DAG
definition and makes it more lean and readable.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedshiftTruncateOperator(BaseOperator):
    """
    Truncates a table in AWS Redshift

    :param schema: reference to a specific schema in Redshift database
    :type schema: str
    :param table: reference to a specific table in Redshift database
    :type table: str
    :param redshift_conn_id: reference to a specific Redshift database
    :type redshift_conn_id: str
    """

    template_fields = ("schema", "table")
    template_ext = ()
    ui_color = "#dddddd"

    @apply_defaults
    def __init__(
        self, schema: str, table: str, redshift_conn_id: str = "redshift_default", *args, **kwargs,
    ) -> None:
        super(RedshiftTruncateOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        truncate_query = f"""
            TRUNCATE {self.schema}.{self.table};
        """

        self.log.info("Truncating table ...")
        postgres_hook.run(truncate_query)
        self.log.info("Truncating table complete ...")
