from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from configuration import config
from datahub.common import alerts
from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator
from operators.utils.resources import ResourcesList

default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=4),
    'executor_config': {
        'KubernetesExecutor': {
            'request_cpu': "200m",
            'limit_cpu': "200m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi",
        },
    },
}

train_resources = ResourcesList(Variable.get('breaks-forecasting-train-resources',
                                             default_var=config['breaks-forecasting']['resources'],
                                             deserialize_json=True))

query_resources = ResourcesList(Variable.get('breaks-forecasting-query-resources',
                                             default_var=config['breaks-forecasting']['resources'],
                                             deserialize_json=True))


base_image = '683110685365.dkr.ecr.eu-west-1.amazonaws.com/breaks-forecasting'

# Env vars configurable via airflow ui.
AIRFLOW_ENV_VARS = Variable.get("breaks-forecasting-env-vars", default_var={}, deserialize_json=True)
# Env vars to pods
env_vars = {'BQ_PROJECT': AIRFLOW_ENV_VARS.get("BQ_PROJECT", "fulfillment-dwh-staging"),
            'FORECAST_API_TOKEN': Variable.get('hurrier-api-token', default_var=""),
            'FORECAST_API_PREFIX': AIRFLOW_ENV_VARS.get("FORECAST_API_PREFIX", ""),
            'S3_MODEL_OUTPUT_BUCKET': Variable.get('data-science-model-outputs-bucket', default_var="")
            }

country_codes = sorted([cc for region in config['regions']
                        for cc in config['regions'][region]['countries']
                        if cc not in AIRFLOW_ENV_VARS.get('DISABLED_COUNTRIES', [])])

dag = DAG(
    dag_id=f'process-breaks-forecast-v1',
    description='Forecast future breaks',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 18 * * *',
    on_failure_callback=alerts.setup_callback(),
    tags=[default_args['owner']],
)

start = DummyOperator(dag=dag, task_id='start')
end = DummyOperator(dag=dag, task_id='end')

extract_breaks_shifts_op = LogisticsKubernetesOperator(
    task_id=f'extract-breaks-and-shifts-data',
    name=f'extract-breaks-and-shifts-data',
    image=base_image,
    env_vars=env_vars,
    cmds=['python', 'cli.py', 'save-extracted-break-shifts'],
    resources=query_resources.get_default(),
    dag=dag
)

distribute_shifts_op = LogisticsKubernetesOperator(
    task_id=f'distribute-extracted-shifts',
    name=f'distribute-extracted-shifts',
    image=base_image,
    env_vars=env_vars,
    cmds=['python', 'cli.py', 'save-distributed-shifts'],
    arguments=[],
    resources=query_resources.get_default(),
    dag=dag
)

distribute_breaks_op = LogisticsKubernetesOperator(
    task_id=f'distribute-extracted-breaks',
    name=f'distribute-extracted-breaks',
    image=base_image,
    env_vars=env_vars,
    cmds=['python', 'cli.py', 'save-distributed-breaks'],
    resources=query_resources.get_default(),
    dag=dag
)

aggregate_breaks_and_shifts_op = LogisticsKubernetesOperator(
    task_id=f'aggregate-breaks-and-shifts',
    name=f'aggregate-breaks-and-shifts',
    image=base_image,
    env_vars=env_vars,
    cmds=['python', 'cli.py', 'save-aggregated-breaks-and-shifts'],
    resources=query_resources.get_default(),
    dag=dag
)

start >> extract_breaks_shifts_op
extract_breaks_shifts_op >> distribute_shifts_op
extract_breaks_shifts_op >> distribute_breaks_op
distribute_shifts_op >> aggregate_breaks_and_shifts_op
distribute_breaks_op >> aggregate_breaks_and_shifts_op

for country_code in country_codes:

    s3_output_key = 'breaks_forecasting/{{ds}}/' + country_code + '/'

    train_save_breaks_forecast_op = LogisticsKubernetesOperator(
        task_id=f'train-save-breaks-forecast-{country_code}',
        name=f'train-save-breaks-forecast-{country_code}',
        image=base_image,
        env_vars=env_vars,
        cmds=['python', 'cli.py', 'forecast-and-save-breaks'],
        arguments=['--country-code', country_code, '--execution-date', '{{ds}}', '--output-key', s3_output_key],
        node_profile='simulator-node',
        resources=train_resources.get(country_code),
        dag=dag
    )

    upload_forecasts_to_hurrier_op = LogisticsKubernetesOperator(
        task_id=f'upload-forecast-to-hurrier-{country_code}',
        name=f'upload-forecast-to-hurrier-{country_code}',
        image=base_image,
        env_vars=env_vars,
        cmds=['python', 'cli.py', 'upload-forecast'],
        arguments=['--country-code', country_code, '--execution-date', '{{ds}}', '--output-key', s3_output_key],
        node_profile='simulator-node',
        resources=train_resources.get(country_code),
        dag=dag
    )

    save_forecasts_to_bq = LogisticsKubernetesOperator(
        task_id=f'save-forecast-to-bq-{country_code}',
        name=f'upload-forecast-to-bq-{country_code}',
        image=base_image,
        env_vars=env_vars,
        cmds=['python', 'cli.py', 'save-forecast-to-bq'],
        arguments=['--output-key', s3_output_key],
        resources=train_resources.get(country_code),
        dag=dag,
        pool=config['pools']['process-breaks-forecast']['name'],
    )

    aggregate_breaks_and_shifts_op >> train_save_breaks_forecast_op >> upload_forecasts_to_hurrier_op
    upload_forecasts_to_hurrier_op >> save_forecasts_to_bq >> end
