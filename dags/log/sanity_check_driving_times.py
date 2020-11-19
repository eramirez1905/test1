import re
from datetime import timedelta, datetime

import pandas as pd
import pendulum
from airflow import DAG
from airflow import configuration

from datahub.common import alerts
from datahub.operators.sanity_check_operator import SanityCheckOperator

default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'executor_config': {
        'KubernetesExecutor': {
            'request_cpu': "200m",
            'limit_cpu': "200m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi",
        }
    }
}

dags_folder = configuration.get('core', 'dags_folder')

DRIVING_TIMES_SLACK_CHANNEL = 'GFKUQUB0B'
VERSION = 1


class DrivingTimesSanityCheckOperator(SanityCheckOperator):
    def intro_text(self, sanity_check_results):
        n_zones = sanity_check_results.shape[0]
        self.post_to_slack(f'*These {n_zones} fleet-vehicle combinations show an increasing error:*\n')

    def format_results(self, sanity_check_results):
        def generate_airflow_link(row):
            lookback_date = (pendulum.instance(row.date) - pendulum.Interval(days=30)).date()

            airflow_link = (f'<https://metabase.usehurrier.com/dashboard/57?'
                            f'hurrier_country_code={row.country_code}'
                            f'&start_date={lookback_date}|Check in Metabase>')

            return airflow_link

        if len(sanity_check_results) == 0:
            return []

        results = []

        # format and add columns for the output on Slack
        sanity_check_results['check_on_metabase'] = sanity_check_results.apply(generate_airflow_link, axis=1)
        sanity_check_results['mae_change_pct_formatted'] = \
            sanity_check_results['mae_change_pct'].apply(lambda x: f'{round(x)}%')

        # prepare to transform dataframe into nicely formatted strings
        pd.set_option('display.max_colwidth', -1)
        cols_to_display = ['vehicle_profile', 'country_code', 'date', 'num_datapoints', 'mae_change_pct_formatted',
                           'check_on_metabase']
        df = sanity_check_results[cols_to_display]

        # split into chunks of 7 to not hit the max char limit for code block display in slack
        chunk_size = 7
        df_chunks = self.split_df(df, chunk_size)

        for df_chunk in df_chunks:
            if len(df_chunk) == 0:
                break

            df_string = df_chunk.to_string(
                index=False,
                header=['vehicle_profile', 'country_code', 'date', 'num_datapoints', 'change_in_MAE', ''],
                justify='left',
                col_space=12
            )

            df_string_removed_trailing_whitespace = re.sub(r'\s+\n', '\n', df_string)

            results.append('```' + df_string_removed_trailing_whitespace + '```')

        return results


dag = DAG(
    dag_id=f'sanity-check-driving-times-v{VERSION}',
    description='Runs a daily sanity check of the driving time estimates.',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 7 * * *',
    template_searchpath=f'{dags_folder}/driving_times/bigquery/sanity_check',
    tags=[default_args['owner']],
)

write_orders_to_s3_operator = DrivingTimesSanityCheckOperator(
    task_id=f'sanity_check_driving_times',
    sql='sanity_check_driving_times.sql',
    slack_channel=DRIVING_TIMES_SLACK_CHANNEL,
    slack_username='Dr. Iving C. Hecker',
    dag=dag,
    on_failure_callback=alerts.setup_callback(),
    on_success_callback=alerts.setup_callback(),
)
