from typing import List

import numpy as np
from airflow import AirflowException
from airflow.hooks.slack_hook import SlackHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from datahub.hooks.bigquery_hook import BigQueryHook


class SanityCheckOperator(BaseOperator):
    """
    Base Class for implementing sanity checks that are posted on slack.

    Functions to be implemented are `intro_text` and `format_results`. Both receive the result of the sanity check
    query.

    `intro_text` should return a small introductory message which will be posted first.

    `format_results` should return a list of strings that will be posted sequentially.
    """
    template_fields = ('sql',)
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self, sql, slack_channel, slack_username,
                 bigquery_conn_id='bigquery_default',
                 slack_conn_id='slack_default', *args, **kwargs):
        self.sql = sql
        self.bigquery_conn_id = bigquery_conn_id
        self.slack_conn_id = slack_conn_id
        self.slack_channel = slack_channel
        self.slack_username = slack_username
        super().__init__(*args, **kwargs)

    def intro_text(self, sanity_check_results) -> str:
        raise NotImplementedError()

    def format_results(self, sanity_check_results) -> List[str]:
        raise NotImplementedError()

    def execute(self, context):
        # calculate variation between order numbers for different job runs
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id, use_legacy_sql=False)

        self.log.info("Running sanity-check query on BigQuery...")
        sanity_check_results = bq_hook.get_pandas_df(sql=self.sql)

        self.post_to_slack(self.intro_text(sanity_check_results))

        chunks = self.format_results(sanity_check_results)

        for chunk in chunks:
            self.post_to_slack(chunk)

    def post_to_slack(self, text):
        hook = SlackHook(slack_conn_id=self.slack_conn_id)
        try:
            hook.call(
                method='chat.postMessage',
                api_params={
                    "channel": self.slack_channel,
                    "username": self.slack_username,
                    "text": text,
                    "icon_url": 'https://www.topimagesystems.com/wp-content/uploads/2015/10/TIS_validate_icon1.png',
                }
            )
        except AirflowException as e:
            self.log.info('Slack API call failed with error message: %s', e)

    def split_df(self, df, chunk_size):
        indices = range(chunk_size, (df.shape[0] // chunk_size + 1) * chunk_size, chunk_size)
        return np.split(df, indices)
