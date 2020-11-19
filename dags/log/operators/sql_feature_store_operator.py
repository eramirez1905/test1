import gzip
import shutil
import logging
from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

import json
import pandas as pd

from operators.utils.validation import validate_data, summarize_validation


class SqlToFeatureStoreOperator(BaseOperator):
    template_fields = ('s3_key', 'sql', 'validation')
    template_ext = ('.sql', '.json')

    @apply_defaults
    def __init__(self, sql, pg_connection_id, s3_bucket_name, s3_key,
                 separator=';',
                 statement_timeout=7200,
                 validation=None,
                 *args, **kwargs):
        self.sql = sql
        self.pg_connection_id = pg_connection_id
        self.s3_bucket_name = s3_bucket_name
        self.s3_key = s3_key
        self.separator = separator
        self.statement_timeout = statement_timeout
        self.validation = validation
        self.log.setLevel(logging.INFO)
        super(SqlToFeatureStoreOperator, self).__init__(*args, **kwargs)

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

            if self.validation is not None:
                # TODO: log level should be info instead of warn
                self.log.warn('Running validation of primary key & schema.')

                # use f.seek to be able to load temp file
                f.seek(0)
                data = pd.read_csv(f, sep=self.separator)

                # read validation json and apply & summarize
                validation = json.loads(self.validation)
                validation_result = validate_data(data, validation)
                validation_summary = summarize_validation(validation_result)

                if not validation_result['is_valid']:
                    self.log.warn(validation_summary)
                    raise ValueError('Validation failed.')
                else:
                    # TODO: log level should be info instead of warn
                    self.log.warn(validation_summary)
            else:
                self.log.warn('No validation supplied, skipping.')

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
