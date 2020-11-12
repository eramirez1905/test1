from configuration import config

FEATURE_STORE_DATASET: str = config.get('feature_store').get('dataset')
QUEUED_ORDERS_TABLE_NAME: str = config.get('feature_store').get('queued_orders_table_name')
LIVE_ORDERS_TABLE_NAME: str = config.get('feature_store').get('live_orders_table_name')
PROJECT_ID: str = config.get('bigquery').get('project_id')
FEATURE_STORE_LABELS: dict = {
    'dag_id': '{{ dag.dag_id }}',
    'task_id': '{{ task.task_id }}',
}

FEATURE_STORE_LIVE_ORDERS_APP_NAME: str = "live_orders"
FEATURE_STORE_QUEUED_ORDERS_APP_NAME: str = "queued_orders"
