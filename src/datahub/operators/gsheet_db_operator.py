import logging

import numpy as np
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from sqlalchemy import create_engine
from sqlalchemy import text

from datahub.hooks.google_sheets_hook import GoogleSheetsHook


class GoogleSheetsToPostgresOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            dwh_conn_id='dwh',
            google_sheets_conn_id='google_sheets_default',
            spreadsheet_id=None,
            spreadsheet_range=None,
            table_to_truncate=None,
            schema_to_insert=None,
            table_to_insert=None,
            *args, **kwargs):

        super(GoogleSheetsToPostgresOperator, self).__init__(*args, **kwargs)

        self._hook = None

        self.dwh_conn_id = dwh_conn_id
        self.google_sheets_conn_id = google_sheets_conn_id

        self.spreadsheet_id = spreadsheet_id
        self.spreadsheet_range = spreadsheet_range

        self.table_to_truncate = table_to_truncate
        self.schema_to_insert = schema_to_insert
        self.table_to_insert = table_to_insert

    # Google Hook to grab the credentials for Google Sheets API
    def _build_hook(self):
        return GoogleSheetsHook(google_cloud_storage_conn_id=self.google_sheets_conn_id)

    @property
    def hook(self):
        if self._hook is None:
            self._hook = self._build_hook()
        return self._hook

    def execute(self, context):
        result = self.hook.download(spreadsheet_id=self.spreadsheet_id,
                                    spreadsheet_range=self.spreadsheet_range)

        # Get all values of Spreadsheet (even with headers)
        values = result.get('values', [])

        # Check if exists values
        if not values:
            logging.warning('No data found.')

        # Get only the headers and body. This code eliminates the indexes in the result.get() operation
        keys = values[0]
        values = [row[:len(values[0])] for row in values[1:]]
        df = pd.DataFrame(values, columns=keys)

        # This statement here removes the 'None' from Pandas implementation and uses the Numpy None
        # that None will be translated to Psycopg as NULL in the database INSERT statement
        df.fillna(value=pd.np.nan, inplace=True)

        # Validate the encoding before to insert inside the database
        self.validate_encoding(df)

        # Remove Nan values
        self.remove_nan_values(df)

        # Create engine to pass to executors
        engine = self.get_engine()

        # Database operations to wipe the table and insert the spreadsheet values
        self.truncate_table(engine, self.table_to_truncate)
        self.insert_records(df, engine, self.schema_to_insert, self.table_to_insert)

    # Validates the encoding pattern of the column, if it's not UTF-8 it will breaks
    @staticmethod
    def validate_encoding(df=None):
        for column in df.columns:
            for idx in df[column].index:
                x = df.get_value(idx, column)
                try:
                    x = x if isinstance(x, str) else str(x).encode('utf-8', 'ignore').decode('utf-8', 'ignore')
                    df.set_value(idx, column, x)
                except Exception:
                    print('encoding error: {0} {1}'.format(idx, column))
                    df.set_value(idx, column, '')
                    continue

    # Remove NaN values from dataframe and let the RDBMS put the value as a NULL
    @staticmethod
    def remove_nan_values(df=None):
        df.replace('nan', np.nan, inplace=True)
        return df

    # Get engine to execute database operations
    def get_engine(self):
        # Get the info from Database connection
        dwh_connection = BaseHook.get_connection(self.dwh_conn_id)

        # Create engine to pass to executors
        engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s/%s' % (dwh_connection.login,
                                                                         dwh_connection.password,
                                                                         dwh_connection.host,
                                                                         dwh_connection.port,
                                                                         dwh_connection.schema))
        return engine

    # Truncate the destination table
    @staticmethod
    def truncate_table(engine=None, table=None):
        sql = text('TRUNCATE TABLE %s' % (table))
        engine.execute(sql)

    # Insert the data from Spreadsheet inside the database
    @staticmethod
    def insert_records(df=None, engine=None, schema=None, table=None):
        df.to_sql(table, engine, schema=schema, if_exists='append', index=False)
