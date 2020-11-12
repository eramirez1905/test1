import os
from datetime import datetime, timedelta

from airflow import DAG

from configuration import config
from datahub.common import alerts
from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator

DAG_VERSION = 3
SCORING_IMAGE = '683110685365.dkr.ecr.eu-west-1.amazonaws.com/rooster-scoring-calculator'
POOL_NAME = 'rider_scoring'

default_args = {
    'owner': 'rider-management',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 10),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'max_active_runs': 1,
    'concurrency': 20,
    'executor_config': {
        'KubernetesExecutor': {
            'request_cpu': "200m",
            'limit_cpu': "200m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi"
        }
    }
}


def create_run_scoring_operator(dag, country, action, image, env_vars):
    task_id = f'run-{action}-for-{country}'
    arguments = ['-jar', 'calculator.jar',
                 '--country', country,
                 '--date', '{{ next_execution_date }}',
                 '--action', action]
    return LogisticsKubernetesOperator(
        image=image,
        env_vars=env_vars,
        cmds=['java'],
        arguments=arguments,
        task_id=task_id,
        name=task_id,
        dag=dag,
        on_failure_callback=alerts.setup_callback(),
        node_profile='batch-node',
        is_delete_operator_pod=True,
        keep_pod_on_extended_retries=True,
        pool=POOL_NAME,
    )


token = os.getenv('HURRIER_API_TOKEN', '')
new_relic_license_key = os.getenv('SCORING_NEW_RELIC_LICENSE_KEY', '')

for region in config['rider-scoring']['regions']:
    # Every 60 minutes
    schedule = '0 * * * *'
    if region == 'mm':
        # Every 30 minutes
        schedule = '0/30 * * * *'

    dag = DAG(
        dag_id=f'rider-scoring-{region}-v{DAG_VERSION}',
        description='Calculate rider scoring and publish it to rooster',
        default_args=default_args,
        catchup=False,
        schedule_interval=schedule,
    )

    countries = config['rider-scoring']['regions'][region]['countries']
    for country in countries:
        base_url_suffix = config['rider-scoring']['base_url_suffix']
        base_url = f'https://{country}{base_url_suffix}.usehurrier.com/api'
        env_vars = {
            'ROOSTER_SCORING_API_URL': f'{base_url}/rooster-scoring',
            'ROOSTER_API_URL': f'{base_url}/rooster',
            'FORECAST_API_URL': f'{base_url}/forecast/v1',
            'RIDER_STATS_API_URL': f'{base_url}/rider-stats',
            'API_ACCESS_TOKEN': token,
        }

        if country == 'sa':
            env_vars['GET_SHIFTS_REQUESTS_LIMIT'] = '1'
            env_vars['GET_EMPLOYEES_REQUESTS_LIMIT'] = '1'

        actions = ['publish', 'live_score']

        if region in ['sa']:
            actions = ['publish']

        image = f'{SCORING_IMAGE}:20.41.0'

        if country == 'hk':
            image = f'{SCORING_IMAGE}:20.41.0-hk-hours'
        elif country == 'ph':
            image = f'{SCORING_IMAGE}:20.41.0-ph-hours'

        if country == 'sg':
            image = f'{SCORING_IMAGE}:20.41.0-minutes-sg'

        is_production = config['rider-scoring']['production']
        if not is_production:
            image = f'{SCORING_IMAGE}:latest'

        for action in actions:
            run_scoring = create_run_scoring_operator(dag, country, action, image, env_vars)

            run_scoring

    globals()[dag.dag_id] = dag
