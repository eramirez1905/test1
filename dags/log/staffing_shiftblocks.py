import datetime

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from datahub.common import alerts
from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator

VERSION = 2

pod_resources = {
    'request_cpu': "2000m",
    'limit_cpu': "2000m",
    'request_memory': "4000Mi",
    'limit_memory': "4000Mi",
}

default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime.datetime(2018, 8, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=4),
    'executor_config': {
        'KubernetesExecutor': {
            'request_cpu': "100m",
            'limit_cpu': "500m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi",
        },
    },
}

env_vars = {
    "FORECAST_API_TOKEN": Variable.get("hurrier-api-token", default_var="skip"),
    "ROOSTER_API_TOKEN": Variable.get("hurrier-api-token", default_var="skip"),
    "SENTRY_DSN": Variable.get("staffing-sentry-endpoint", default_var="skip"),
}

# This variable defines the schedule for the countries. The variable has to be
# in JSON format with the following schema:
#     [
#       {
#           "country_code":"ar",
#           "time":"09:30:00"
#       },
#       {
#           "country_code":"bo",
#           "time":"09:30:00"
#       },
#       {
#           "country_code":"sg",
#           "time": ["09:30:00", "12:30:00"]
#       }
#     ]
schedules = Variable.get("staffing-shiftblocks-schedules", deserialize_json=True,
                         default_var={})


# Returns a callable that checks whether a schedule for a country is defined
# and is supposed to be run within the last hour. Airflow passes all default
# macros to the callable via kwargs.
def check_schedule(schedule):
    def check(**kwargs):
        now = kwargs["next_execution_date"]

        schedule_times = schedule["time"]
        if not isinstance(schedule_times, list):
            schedule_times = [schedule_times]

        schedule_times = [pendulum.parse(time).time() for time in schedule_times]

        country_code = schedule["country_code"]

        times_to_run = [datetime.datetime.combine(now.date(), schedule_time).timestamp()
                        for schedule_time in schedule_times]

        now_ts = now.timestamp()
        is_time_to_run = any([now_ts <= time_to_run < now_ts + datetime.timedelta(hours=1).total_seconds()
                              for time_to_run in times_to_run])

        if is_time_to_run:
            return f"run-staffing-shiftblocks-{country_code}"
        else:
            return f"skip-staffing-shiftblocks-{country_code}"

    return check


dag = DAG(
    dag_id=f"staffing-shiftblocks-v{VERSION}",
    description='Create empty shiftblocks',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 * * * *',
    tags=[default_args['owner']],
)

start = DummyOperator(
    task_id="start",
    dag=dag,
)

for schedule in schedules:
    country_code = schedule["country_code"]

    rooster_url = f"https://{country_code}.usehurrier.com./api/rooster/v2/"
    forecast_url = f"https://{country_code}.usehurrier.com./api/forecast/v1/"

    country_env_vars = env_vars.copy()
    country_env_vars.update({
        "COUNTRY": country_code,
        "ROOSTER_API_URL": rooster_url,
        "FORECAST_API_URL": forecast_url,
    })
    country_env_vars.update(schedule.get("env_vars", {}))

    conditional_operator = BranchPythonOperator(
        task_id=f"check-schedule-{country_code}",
        python_callable=check_schedule(schedule),
        provide_context=True,   # this will automatically pass default macros
        dag=dag,
    )

    dummy_operator = DummyOperator(
        task_id=f"skip-staffing-shiftblocks-{country_code}",
        dag=dag,
    )

    country_operator = LogisticsKubernetesOperator(
        image=("683110685365.dkr.ecr.eu-west-1.amazonaws.com/rooster-staffing-stateless"),
        image_pull_policy="Always",
        env_vars=country_env_vars,
        cmds=["bin/staffing"],
        arguments=["unassigned-shifts",
                   # the timeframe for shiftblocks starts tomorrow
                   "--date_start",
                   "{{ (execution_date + macros.timedelta(days=1)).date() }}",
                   # the timeframe ends on the sunday of the week after tomorrow
                   "--date_end",
                   "{{ (execution_date + macros.dateutil.relativedelta.relativedelta(days=13, weekday=macros.dateutil.relativedelta.MO)).date() }}",
                   "--execution_date",
                   "{{ ts }}"],
        name=f"run-staffing-shiftblocks-{country_code}",
        task_id=f"run-staffing-shiftblocks-{country_code}",
        resources=pod_resources,
        dag=dag,
        node_profile="batch-node",
        is_delete_operator_pod=True,
        on_success_callback=alerts.setup_callback(),
        on_failure_callback=alerts.setup_callback(),
    )

    start >> conditional_operator >> (country_operator, dummy_operator)
