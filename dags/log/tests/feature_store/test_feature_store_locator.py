import unittest

from airflow import AirflowException

from feature_store import feature_store_locator, FEATURE_STORE_QUEUED_ORDERS_APP_NAME, QUEUED_ORDERS_TABLE_NAME, LIVE_ORDERS_TABLE_NAME, \
    FEATURE_STORE_LIVE_ORDERS_APP_NAME


class TestFeatureStoreLocator(unittest.TestCase):

    def test_resolve_finds_queued_orders(self):
        self.assertEqual(feature_store_locator.resolve(FEATURE_STORE_QUEUED_ORDERS_APP_NAME), {
            'sql_create_view_backfill': 'feature_store_real_time_view.sql',
            'sql_create_table': 'queued_orders_create.sql',
            'sql_run_backfill': 'queued_orders_snapshot.sql',
            'sql_create_view_daily': 'feature_store_real_time_view.sql',
            'sql_create_staging_table': 'queued_orders_snapshot.sql',
            'table_name': QUEUED_ORDERS_TABLE_NAME,
        })

    def test_resolve_finds_live_orders(self):
        self.assertEqual(feature_store_locator.resolve(FEATURE_STORE_LIVE_ORDERS_APP_NAME), {
            'sql_create_view_backfill': 'live_orders_backfill.sql',
            'sql_create_table': 'live_orders_create.sql',
            'sql_run_backfill': 'live_orders_snapshot.sql',
            'sql_create_view_daily': 'live_orders_backfill.sql',
            'sql_create_staging_table': 'live_orders_snapshot.sql',
            'table_name': LIVE_ORDERS_TABLE_NAME,
        })

    def test_resolve_raises_exception_when_app_not_found(self):
        with self.assertRaises(AirflowException) as error_message:
            feature_store_locator.resolve("dynamic_pricing")

        self.assertEqual(str(error_message.exception), 'The following app name has not been defined: dynamic_pricing')
