from cost_monitoring.slack_helper import SlackHelper
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from datahub.hooks.bigquery_hook import BigQueryHook
from cost_monitoring.slack_block_generator import block_generator

from airflow.utils.decorators import apply_defaults
from airflow.hooks.slack_hook import SlackClient

from datahub.common.helpers import grouper
from typing import Optional


class CostMonitoringOperator(BigQueryOperator):

    @apply_defaults
    def __init__(self,
                 config,
                 sql,
                 bigquery_conn_id='bigquery_default',
                 slack_conn_id: Optional[str] = None,
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(sql=sql, *args, **kwargs)
        self.config = config
        self.sql = sql
        self.bigquery_conn_id = bigquery_conn_id
        self.slack_conn_id = slack_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        big_query_hook = BigQueryHook(
            bigquery_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to,
            use_legacy_sql=False,
        )

        expensive_queries = big_query_hook.get_pandas_df(sql=self.sql).to_dict(orient='records')

        slack = SlackClient(
            token=self.config['slack']['cost_monitoring_token'],
            slack_conn_id=self.slack_conn_id,
        )
        slack_helper = SlackHelper(slack=slack)
        generated_blocks = block_generator(responses=expensive_queries, slack=slack_helper)
        grouped_blocks = grouper(generated_blocks, 50, {})  # API limit of 50 elements per block

        for block in grouped_blocks:
            blocks = list(filter(lambda x: x, block))
            slack_helper.post_message(
                channel=self.config['slack'].get("channel_cost_monitoring").get("name"),
                username='Cost Monitoring',
                blocks=blocks,
            )
