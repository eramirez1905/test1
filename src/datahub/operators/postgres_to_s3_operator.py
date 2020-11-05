"""
Operator to extract Postgres SQL to S3.

Loosely based on:
https://github.com/apache/airflow/blob/master/airflow/providers/google/cloud/transfers/postgres_to_gcs.py
"""

from datahub.operators.sql_to_s3_operator import BaseSQLToS3Operator

from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults


class PostgresToS3Operator(BaseSQLToS3Operator):
    """
    Copy data from Postgres to S3 in JSON or CSV format.
    :param postgres_conn_id: Reference to a specific Postgres connection.
    :type postgres_conn_id: str
    """

    ui_color = "#a0e08c"

    @apply_defaults
    def __init__(self, postgres_conn_id="postgres_default", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def query(self):
        """
        Queries Postgres and returns a cursor to the results.
        """
        self.log.info("Executing: %s", self.sql)
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql, self.parameters)
        return cursor
