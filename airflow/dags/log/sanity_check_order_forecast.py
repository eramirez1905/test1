import re
from datetime import timedelta, datetime

import pandas as pd
from airflow import DAG
from airflow import configuration

from datahub.common import alerts
from datahub.operators.sanity_check_operator import SanityCheckOperator

default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 25),
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

ORDER_FORECAST_CHANNEL = 'G5DHN166M'
VERSION = 1


class OrderForecastSanityCheckOperator(SanityCheckOperator):
    def intro_text(self, sanity_check_results):
        n_zones = sanity_check_results.shape[0]
        if n_zones > 0:
            self.post_to_slack(f'*These {n_zones} zones show high variation between job runs:*\n')
        else:
            self.post_to_slack(f'*No high variation between job runs detected.*')

    def format_results(self, sanity_check_results):
        def generate_airflow_link(row):
            airflow_link = (f'<https://metabase.usehurrier.com/question/147'
                            f'?country_code={row.country_code}'
                            f'&zone_id={row.zone_id}|Metabase>')
            return airflow_link

        if len(sanity_check_results) == 0:
            return []

        results = []

        # format and add columns for the output on Slack
        sanity_check_results['check_on_grafana'] = sanity_check_results.apply(generate_airflow_link, axis=1)
        sanity_check_results['stddev_percent_between_job_runs_formatted'] = \
            sanity_check_results['stddev_percent_between_job_runs'].apply(lambda x: f'{round(x*100)}%')
        sanity_check_results['avg_orders_per_day_formatted'] = \
            sanity_check_results['avg_orders_per_30_mins'].apply(lambda x: f'{round(x*48)}')

        # prepare to transform dataframe into nicely formatted strings
        pd.set_option('display.max_colwidth', -1)
        cols_to_display = ['country_code', 'zone_id', 'geo_id', 'shape_updated_at', 'avg_orders_per_day_formatted',
                           'stddev_percent_between_job_runs_formatted', 'check_on_grafana']
        df = sanity_check_results[cols_to_display]

        df.reset_index()

        # cut geo_id for display purposes
        df['geo_id'] = df['geo_id'].str[0:20]

        # split into chunks of 7 to not hit the max char limit for code block display in slack
        chunk_size = 7
        df_chunks = self.split_df(df, chunk_size)

        for df_chunk in df_chunks:
            if len(df_chunk) == 0:
                break

            df_string = df_chunk.to_string(
                index=False,
                header=['country', 'zone_id', 'geo_id', 'geojson_updated', 'avg_orders', 'std_perc', ''],
                justify='left',
                col_space=12
            )

            df_string_removed_trailing_whitespace = re.sub(r'\s+\n', '\n', df_string)

            results.append('```' + df_string_removed_trailing_whitespace + '```')

        return results


class OrderForecastLastJobRunCheckOperator(SanityCheckOperator):
    def intro_text(self, sanity_check_results):
        n_countries = sanity_check_results.shape[0]
        if n_countries > 0:
            self.post_to_slack(f'Job runs are outdated (older than 72 hours) in the following {n_countries} countries:')
        else:
            pass

    def format_results(self, sanity_check_results):
        if sanity_check_results.shape[0] > 0:
            pd.set_option('display.max_colwidth', -1)
            cols = ['country_code', 'last_order_placed_at', 'last_job_run_at', 'last_job_run_hours_ago',
                    'previous_job_run_at', 'orders_change']
            results = sanity_check_results[cols]
            results.reset_index()
            results_str = results.to_string(
                index=False,
                header=cols,
                justify='left',
                col_space=16
            )
            results_str = re.sub(r'\s+\n', '\n', results_str)
            # needs to return a list, otherwise goes crazy printing character by character
            return ['```' + results_str + '```']
        else:
            return []


dag = DAG(
    dag_id=f'sanity-check-order-forecast-v{VERSION}',
    description='Runs a daily sanity check of the order forecast by calculating '
                'variation between job runs and time since last job run.',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 7 * * *',
    template_searchpath=f'{dags_folder}/forecast/bigquery/sanity_check',
    tags=[default_args['owner']],
)

check_forecast_increments_operator = OrderForecastSanityCheckOperator(
    task_id=f'sanity_check_order_forecast',
    sql='compare_forecasting_runs.sql',
    slack_channel=ORDER_FORECAST_CHANNEL,
    slack_username='Order forecast check',
    dag=dag,
    on_failure_callback=alerts.setup_callback(),
    on_success_callback=alerts.setup_callback(),
)

check_latest_job_runs_operator = OrderForecastLastJobRunCheckOperator(
    task_id=f'sanity_check_last_job_runs',
    sql='last_job_runs.sql',
    slack_channel=ORDER_FORECAST_CHANNEL,
    slack_username='Order forecast check',
    dag=dag,
    on_failure_callback=alerts.setup_callback(),
    on_success_callback=alerts.setup_callback(),
)
