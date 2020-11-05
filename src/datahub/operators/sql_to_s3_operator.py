"""
Base operator for SQL to S3 operators.

Loosely based on
https://github.com/apache/airflow/blob/master/airflow/providers/google/cloud/transfers/sql_to_gcs.py
"""

import abc
import json
from datetime import date, datetime
import decimal

import warnings
from tempfile import NamedTemporaryFile

import unicodecsv as csv

from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.decorators import apply_defaults


class BaseSQLToS3Operator(BaseOperator):
    """
    :param sql: The SQL to execute.
    :type sql: str
    :param s3_bucket: The S3 bucket to upload to.
    :type s3_bucket: str
    :param s3_key: The S3 key to use as the object name when uploading to AWS S3 including
        prefixes. A ``{}`` should be specified in the filename to allow the operator to inject
        file numbers in cases where the file is split due to size.
        For example ``foo/bar_{}.csv``
    :type s3_key: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files. This param allows developers to specify the
        file size of the splits.
    :type approx_max_file_size_bytes: long
    :param export_format: Desired format of files to be exported. Can be csv or json.
        NOTE: When using JSON format Decimals are converted to floats. Which can imply a loss of
        precision. Use with caution.
    :type export_format: str
    :param field_delimiter: The delimiter to be used for CSV files.
    :type field_delimiter: str
    :param replace: A flag to decide whether or not to overwrite the key
        if it already exists. If replace is False and the key exists, an
        error will be raised.
    :type replace: bool
    :param encrypt: If True, the file will be encrypted on the server-side
        by S3 and will be stored in an encrypted form while at rest in S3.
    :type encrypt: bool
    :param gzip: Option to compress file for upload (does not apply to schemas).
    :type gzip: bool
    :param aws_conn_id: (Optional) The connection ID used to connect to AWS S3.
    :type aws_conn_id: str
    :param parameters: a parameters dict that is substituted at query runtime.
    :type parameters: dict
    """

    template_fields = ("sql", "s3_bucket", "s3_key", "parameters")
    template_ext = (".sql",)
    ui_color = "#a0e08c"

    @apply_defaults
    def __init__(
        self,  # pylint: disable=too-many-arguments
        sql,
        s3_bucket,
        s3_key,
        approx_max_file_size_bytes=1900000000,
        export_format="json",
        field_delimiter=",",
        replace=False,
        encrypt=False,
        gzip=False,
        parameters=None,
        aws_conn_id="aws_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.sql = sql
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.export_format = export_format.lower()
        self.field_delimiter = field_delimiter
        self.replace = replace
        self.encrypt = encrypt

        from airflow import __version__ as airflow_version  # noqa

        if gzip and (airflow_version <= "1.10.11"):
            warnings.warn(
                "gzip option of BaseSQLToS3Operator is currently disabled due to a bug in the "
                "underlying S3hook. "
                "See: https://github.com/apache/airflow/pull/7680#issuecomment-619763051",
                RuntimeWarning,
            )
            self.gzip = False
        else:
            self.gzip = gzip
        self.parameters = parameters
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        self.log.info("Executing query")
        cursor = self.query()

        self.log.info("Writing local data files")
        files_to_upload = self._write_local_data_files(cursor)

        # Flush all files before uploading
        for tmp_file in files_to_upload:
            tmp_file["file_handle"].flush()

        self.log.info("Uploading %d files to S3.", len(files_to_upload))
        self._upload_to_s3(files_to_upload)

        self.log.info("Removing local files")
        # Close all temp file handles.
        for tmp_file in files_to_upload:
            tmp_file["file_handle"].close()

    def _json_serializer(self, obj):
        """
        JSON serializer for not serializable by default JSON code.
        Serializes
        - date and datetime objects to ISO8601 strings
        - decimal objects to floats
        See
        - https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable
        - https://stackoverflow.com/questions/1960516/python-json-serialize-a-decimal-object/
        """

        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        raise TypeError("Type %s not serializable" % type(obj))

    def _write_local_data_files(self, cursor):
        """
        Takes a cursor, and writes results to a local file.

        :return: A list of dictionaries with information about filenames to be used as object
            names in S3 and file handles to local files that contain the data for the S3 objects.
        """
        schema = list(map(lambda schema_tuple: schema_tuple[0], cursor.description))
        file_no = 0
        tmp_file_handle = NamedTemporaryFile(delete=True)
        if self.export_format == "csv":
            file_mime_type = "text/csv"
        else:
            file_mime_type = "application/json"
        files_to_upload = [
            {
                "file_name": self.s3_key.format(file_no),
                "file_handle": tmp_file_handle,
                "file_mime_type": file_mime_type,
            }
        ]
        self.log.info("Current file count: %d", len(files_to_upload))

        if self.export_format == "csv":
            csv_writer = self._configure_csv_file(tmp_file_handle, schema)

        for row in cursor:
            if self.export_format == "csv":
                csv_writer.writerow(row)
            else:
                row_dict = dict(zip(schema, row))

                tmp_file_handle.write(
                    json.dumps(
                        row_dict, sort_keys=True, ensure_ascii=False, default=self._json_serializer
                    ).encode("utf-8")
                )

                # Append newline to make dumps BigQuery compatible.
                tmp_file_handle.write(b"\n")

            # Stop if the file exceeds the file size limit.
            if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                file_no += 1
                tmp_file_handle = NamedTemporaryFile(delete=True)
                files_to_upload.append(
                    {
                        "file_name": self.filename.format(file_no),
                        "file_handle": tmp_file_handle,
                        "file_mime_type": file_mime_type,
                    }
                )
                self.log.info("Current file count: %d", len(files_to_upload))
                if self.export_format == "csv":
                    csv_writer = self._configure_csv_file(tmp_file_handle, schema)

        return files_to_upload

    def _configure_csv_file(self, file_handle, schema):
        """Configure a csv writer with the file_handle and write schema
        as headers for the new file.
        """
        csv_writer = csv.writer(file_handle, encoding="utf-8", delimiter=self.field_delimiter)
        csv_writer.writerow(schema)
        return csv_writer

    @abc.abstractmethod
    def query(self):
        """Execute DBAPI query."""

    def _upload_to_s3(self, files_to_upload):
        """
        Upload all of the file splits to AWS S3.
        """
        hook = S3Hook(aws_conn_id=self.aws_conn_id)

        for tmp_file in files_to_upload:
            hook.load_file(
                filename=tmp_file.get("file_handle").name,
                key=tmp_file.get("file_name"),
                bucket_name=self.s3_bucket,
                replace=self.replace,
                encrypt=self.encrypt,
                gzip=self.gzip,
            )
