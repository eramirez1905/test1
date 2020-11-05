import gzip
import shutil
import warnings
from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook


class SqlToS3Operator(BaseOperator):

    template_fields = ('s3_key', 'sql')
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self, sql, pg_connection_id, s3_bucket_name, s3_key, separator=';', statement_timeout=7200,
                 *args, **kwargs):
        self.sql = sql
        self.pg_connection_id = pg_connection_id
        self.s3_bucket_name = s3_bucket_name
        self.s3_key = s3_key
        self.separator = separator
        self.statement_timeout = statement_timeout
        super(SqlToS3Operator, self).__init__(*args, **kwargs)

        warnings.warn(
            "The SqlToS3Operator has been deprecated and will be removed in a future version of "
            "datahub-airflow. Please migrate existing jobs to PostgresToS3Operator instead.",
            FutureWarning
        )

    def execute(self, context):
        # Read data
        postgres_hook = PostgresHook(postgres_conn_id=self.pg_connection_id)
        cur = postgres_hook.get_cursor()
        outputquery = f"COPY ({self.sql}) TO STDOUT WITH CSV HEADER DELIMITER '{self.separator}'"
        s3_hook = S3Hook()

        with NamedTemporaryFile() as f:
            cur.execute(f"SET statement_timeout TO {self.statement_timeout * 1000};")
            cur.copy_expert(outputquery, f)
            cur.execute(f"RESET statement_timeout;")

            # Save data to S3
            with NamedTemporaryFile() as f_zip:
                with gzip.GzipFile(fileobj=f_zip, mode='wb') as f_out:
                    f.seek(0)
                    shutil.copyfileobj(f, f_out)
                f_zip.seek(0)

                s3_hook.load_file(filename=f_zip.name,
                                  key=self.s3_key,
                                  replace=True,
                                  bucket_name=self.s3_bucket_name)
