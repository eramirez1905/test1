from airflow import AirflowException

from feature_store import FEATURE_STORE_LIVE_ORDERS_APP_NAME, FEATURE_STORE_QUEUED_ORDERS_APP_NAME, QUEUED_ORDERS_TABLE_NAME, LIVE_ORDERS_TABLE_NAME

_feature_store_apps = {
    FEATURE_STORE_QUEUED_ORDERS_APP_NAME: {
        'sql_create_view_backfill': 'feature_store_real_time_view.sql',
        'sql_create_table': 'queued_orders_create.sql',
        'sql_run_backfill': 'queued_orders_snapshot.sql',
        'sql_create_view_daily': 'feature_store_real_time_view.sql',
        'sql_create_staging_table': 'queued_orders_snapshot.sql',
        'table_name': QUEUED_ORDERS_TABLE_NAME,
    },
    FEATURE_STORE_LIVE_ORDERS_APP_NAME: {
        'sql_create_view_backfill': 'live_orders_backfill.sql',
        'sql_create_table': 'live_orders_create.sql',
        'sql_run_backfill': 'live_orders_snapshot.sql',
        'sql_create_view_daily': 'live_orders_backfill.sql',
        'sql_create_staging_table': 'live_orders_snapshot.sql',
        'table_name': LIVE_ORDERS_TABLE_NAME,
    },
}


# Inverse of control pattern
def resolve(app_name: str) -> dict:
    result = _feature_store_apps.get(app_name)
    if result is None:
        raise AirflowException(f'The following app name has not been defined: {app_name}')
    return result
