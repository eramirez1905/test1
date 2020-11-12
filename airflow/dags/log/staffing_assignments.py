import datetime

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator

from configuration import config
from datahub.common import alerts
from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator

VERSION = 2
WEEKDAYS = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]

pod_resources = {
    'request_cpu': "1000m",
    'limit_cpu': "1000m",
    'request_memory': "8000Mi",
    'limit_memory': "8000Mi",
}

default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime.datetime(2018, 8, 27),
    'retries': 0,
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
    "GRB_ACCESS_ID": Variable.get("gurobi-access-id", default_var="skip"),
    "GRB_SECRET_KEY": Variable.get("gurobi-secret-key", default_var="skip"),
    "DATA_LAKE_BUCKET": Variable.get("data-science-data-lake-bucket", default_var="skip"),
    "MAX_POSSIBLE_ASSIGNMENTS": Variable.get("staffing-assignments-max-possible", default_var=1000000),
    "USE_POSSIBILITIES_SAMPLING": Variable.get("staffing-assignments-possibilities-sampling", default_var="true")
}

# This variable defines the schedule for the countries. The variable has to be
# in JSON format with the following schema:
#     [{
#         "country_code": "de",
#         "time": "13:00:00",
#         "weekday": "THURSDAY",
#         "city_ids": [1, 2],
#         "week_offset": 1,
#         "env_vars": {
#             "STAFFING_GRID_SIZE": "30"
#         }
#     }]
# i.e. a list objects with time and weekday defining the schedule and city_ids
# specifying which cities to run. The week offset and env_vars are optional.
schedules = Variable.get("staffing-assignments-schedules", deserialize_json=True,
                         default_var={})


# Returns a callable that checks whether a schedule for a country is defined
# and is supposed to be run within the last hour. Airflow passes all default
# macros to the callable via kwargs.
# The next step in the DAG is "end" for skipping and the name of the
# kubernetes operator otherwise.
def check_schedule(schedule):
    def check(**kwargs):
        now = kwargs["next_execution_date"]
        schedule_time = pendulum.parse(schedule["time"]).time()

        time_to_run = datetime.datetime.combine(now.date(), schedule_time).timestamp()

        now_ts = now.timestamp()
        is_time_to_run = now_ts <= time_to_run < now_ts + datetime.timedelta(hours=1).total_seconds()

        is_day_to_run = WEEKDAYS[now.weekday()] == schedule["weekday"]

        return is_time_to_run and is_day_to_run

    return check


dag = DAG(
    dag_id=f"staffing-assignments-v{VERSION}",
    description='Create assignments',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 * * * *',
    tags=[default_args['owner']],
)

start = DummyOperator(
    task_id="start",
    dag=dag,
)

for schedule_id, schedule in enumerate(schedules):
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

    conditional_operator = ShortCircuitOperator(
        task_id=f"check-schedule-{country_code}-{schedule_id}",
        python_callable=check_schedule(schedule),
        provide_context=True,  # this will automatically pass default macros
        dag=dag,
        retries=2,
    )

    start >> conditional_operator

    staffing_image = Variable.get(
        "staffing-assignments-image",
        default_var='683110685365.dkr.ecr.eu-west-1.amazonaws.com/rooster-staffing-stateless:latest'
    )

    for city_id in schedule["city_ids"]:
        city_operator = LogisticsKubernetesOperator(
            image=staffing_image,
            image_pull_policy="Always",
            env_vars=country_env_vars,
            cmds=["bin/staffing"],
            arguments=["assignments",
                       # assignments are run for next week
                       "--week",
                       "{{ (execution_date + macros.dateutil.relativedelta.relativedelta(days=1, weekday=macros.dateutil.relativedelta.MO) + macros.timedelta(weeks=" + str(
                           schedule.get("week_offset", 0)) + ")).date() }}",
                       "--city_id",
                       str(city_id)],
            name=f"run-staffing-assignments-container-{country_code}-{city_id}",
            task_id=f"run-staffing-assignments-container-{country_code}-{city_id}",
            resources=pod_resources,
            dag=dag,
            on_success_callback=alerts.setup_callback(),
            on_failure_callback=alerts.setup_callback(),
            node_profile="batch-node",
            is_delete_operator_pod=True,
            pool=config['pools']['staffing_assignments']['name'],
            keep_pod_on_extended_retries=True,
        )

        conditional_operator >> city_operator
