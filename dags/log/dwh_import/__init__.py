from datetime import timedelta

DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': None,
    'end_date': None,
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'concurrency': 20,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=2),
    'executor_config': {
        'KubernetesExecutor': {
            'request_cpu': "200m",
            'limit_cpu': "200m",
            'request_memory': "500Mi",
            'limit_memory': "500Mi",
        },
    },
}
