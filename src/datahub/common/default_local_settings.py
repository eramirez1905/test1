from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

handler = {
    'console_task': {
        'class': 'logging.StreamHandler',
        'formatter': 'airflow',
        'stream': 'ext://sys.stdout'
    },
}

DEFAULT_LOGGING_CONFIG['handlers'].update(handler)
DEFAULT_LOGGING_CONFIG['loggers']['airflow.task']['handlers'].append('console_task')
