import json
import tenacity
from time import sleep
from datahub.common.helpers import grouper

from airflow import AirflowException
from airflow.utils.decorators import apply_defaults

from datahub.hooks.bigquery_hook import BigQueryHook
from datahub.hooks.extended_http_hook import HttpHookExtended
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator


class BigQueryToBrazeOperator(BigQueryOperator):

    @apply_defaults
    def __init__(self,
                 sql,
                 http_conn_id='braze',
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(sql=sql, *args, **kwargs)
        self.sql = sql
        self.http_conn_id = http_conn_id
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.headers = {'Content-Type': 'application/json'}
        self.retry_args = dict(
            wait=tenacity.wait_exponential(multiplier=1),
            stop=tenacity.stop_after_attempt(6),
            retry=tenacity.retry_if_exception_type(Exception),
        )
        self.api_limit = 75

    def execute(self, context):
        hook = BigQueryHook(
            bigquery_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to,
        )
        df = hook.get_pandas_df(sql=self.sql)
        df = df.fillna('')

        braze_dict = df.to_dict(orient='records')
        braze_list_of_rows = grouper(braze_dict, self.api_limit, {})  # We group by 75 attr., the API Limit for updates.

        # API
        api_hook = HttpHookExtended(http_conn_id=self.http_conn_id, method='POST')
        connection_credentials = api_hook.get_connection(self.http_conn_id)

        for rows in braze_list_of_rows:
            self.post_to_braze(connection_credentials.password, rows, api_hook)
        self.log.info("Finished all POST request to Braze.")

    def post_to_braze(self, api_key, attributes, api_hook):
        payload = {
            "api_key": api_key,
            "attributes": attributes
        }
        json_payload = json.dumps(payload)
        try:
            api_hook.run_with_advanced_retry(
                endpoint='users/track',
                data=json_payload,
                headers=self.headers,
                _retry_args=self.retry_args
            )
            sleep(0.1)
        except Exception:
            raise AirflowException(f'Failed to POST to braze with payload {json_payload}')
