import gzip
import os.path
import pandas as pd
import boto3

from typing import List
from io import BytesIO, TextIOWrapper

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook


s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


class HistoricalForecastS3ToS3Operator(BaseOperator):

    template_fields = ('run_date', 's3_input_folder', 's3_output_key')
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 country_code: str,
                 model_name: str,
                 run_date: str,
                 ndays: int,
                 s3_input_bucket: str,
                 s3_input_folder: str,
                 s3_output_bucket: str,
                 s3_output_key: str,
                 use_columns: List[str],
                 separator: str = ";",
                 *args,
                 **kwargs):

        self.country_code = country_code
        self.model_name = model_name
        self.run_date = run_date
        self.ndays = ndays
        self.s3_input_bucket = s3_input_bucket
        self.s3_input_folder = s3_input_folder
        self.s3_output_bucket = s3_output_bucket
        self.s3_output_key = s3_output_key
        self.use_columns = use_columns
        self.separator = separator
        super(HistoricalForecastS3ToS3Operator, self).__init__(*args, **kwargs)

    def execute(self, context):

        # Read data
        data = []
        s3_hook = S3Hook()
        for d in pd.date_range(pd.to_datetime(self.run_date) - pd.to_timedelta("%d days" % (self.ndays + 1)),
                               pd.to_datetime(self.run_date) - pd.to_timedelta("1 day")):
            s3_input_key = os.path.join(self.s3_input_folder, str(d.date()), self.country_code,
                                        self.model_name, 'forecasts.csv.gz')
            if s3_hook.check_for_key(key=s3_input_key, bucket_name=self.s3_input_bucket):
                obj = s3_client.get_object(Bucket=self.s3_input_bucket, Key=s3_input_key)
                with gzip.GzipFile(fileobj=obj['Body']) as gz_file:
                    daily_data = pd.read_csv(gz_file, sep=self.separator, dtype=str)
                    daily_data = daily_data[list(set(daily_data.columns) & set(self.use_columns))]
                    data.append(daily_data)

        # Output data
        if len(data):
            with BytesIO() as gz_buffer:
                with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
                    pd.concat(data, sort=False).to_csv(TextIOWrapper(gz_file, 'utf8'), sep=self.separator, index=False)
                s3_object = s3_resource.Object(self.s3_output_bucket, self.s3_output_key)
                s3_object.put(Body=gz_buffer.getvalue())
