import argparse
import signal
from datetime import datetime

import jinja2
import pytz
from airflow import configuration
from airflow.hooks.postgres_hook import PostgresHook
from dateutil.parser import parse as datetime_parser

from configuration import config
from datahub.common.helpers import get_last_dag_run_next_execution_date, get_last_dag_run_execution_date

parser = argparse.ArgumentParser()

parser.add_argument('-f', '--sql-file', action="store", dest="sql_file", help='SQL file to execute.', required=True)
parser.add_argument('-s', '--start-datetime', action="store", dest="start_datetime", help='Start datetime', required=True)
parser.add_argument('-e', '--end-datetime', action="store", dest="end_datetime", help='End datetime', required=True)
arguments = parser.parse_args()

sql_file = arguments.sql_file
execution_date = datetime_parser(arguments.start_datetime).isoformat()
next_execution_date = datetime_parser(arguments.end_datetime).isoformat()

jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(configuration.get('core', 'airflow_home')),
                               cache_size=0, autoescape=False)
jinja_env.globals.update({
    'last_dag_run_next_execution_date': get_last_dag_run_next_execution_date,
    'last_dag_run_execution_date': get_last_dag_run_execution_date,
})

countries = config['regions']['us']['countries']
terminate = False

hook = PostgresHook(postgres_conn_id='dwh')


def signal_handler(sig, frame):
    global terminate
    terminate = True


signal.signal(signal.SIGINT, signal_handler)

for region in config['regions']:
    for country in config['regions'][region]['countries']:
        default_params = {
            'execution_date': execution_date,
            'next_execution_date': next_execution_date,
            'params': {
                'country_codes': f'\'{country}\'',
                'interval': '6 week',
                'max_data_retention': '7 month',
            },
            'user_defined_macros': {
                'last_dag_run_next_execution_date': get_last_dag_run_next_execution_date,
                'last_dag_run_execution_date': get_last_dag_run_execution_date,
            },

        }

        sql = jinja_env.get_template(sql_file).render(**default_params)
        # now = datetime.now()
        start = datetime.now(tz=pytz.timezone('Europe/Berlin'))
        start_string = start.strftime('%Y-%m-%d %H:%M:%S')
        print(f'{country} start at {start_string}')

        hook.run(sql, True)

        end = datetime.now(tz=pytz.timezone('Europe/Berlin'))
        end_string = end.strftime('%Y-%m-%d %H:%M:%S')
        duration = end - start
        print(f'{country} end at   {end_string} - duration {duration}')
        if terminate:
            break
    if terminate:
        break
