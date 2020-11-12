from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator

from datahub.common import alerts
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from feature_store import FEATURE_STORE_QUEUED_ORDERS_APP_NAME, FEATURE_STORE_LIVE_ORDERS_APP_NAME
from feature_store.feature_store_pipeline import PROJECT_ID, FEATURE_STORE_DATASET, create_daily_pipeline, create_backfill_pipeline


class FeatureStore:

    def __init__(self,
                 full_import: bool):
        self.full_import: bool = full_import

    @staticmethod
    def import_backfill(start: BaseOperator) -> None:
        queued_orders_start = create_backfill_pipeline(app_name=FEATURE_STORE_QUEUED_ORDERS_APP_NAME)
        live_orders_start = create_backfill_pipeline(app_name=FEATURE_STORE_LIVE_ORDERS_APP_NAME)

        start >> [queued_orders_start, live_orders_start]

    @staticmethod
    def import_daily(start: BaseOperator) -> None:

        # New logic
        feature_store_real_time_query_view_name: str = 'feature_store_real_time'
        feature_store_real_time_query: BigQueryOperator = BigQueryOperator(
            task_id=f'create-view-{feature_store_real_time_query_view_name}',
            sql='feature_store_real_time_view.sql',
            params={
                'project_id': PROJECT_ID,
                'feature_store_dataset': FEATURE_STORE_DATASET,
                'view_name': feature_store_real_time_query_view_name,
                'real_time': True,
                'daily': False,
            },
            use_legacy_sql=False,
            on_failure_callback=alerts.setup_callback(),
        )

        queued_orders_start: BaseOperator = create_daily_pipeline(app_name=FEATURE_STORE_QUEUED_ORDERS_APP_NAME)
        live_orders_start: BaseOperator = create_daily_pipeline(app_name=FEATURE_STORE_LIVE_ORDERS_APP_NAME)

        start >> [feature_store_real_time_query, queued_orders_start, live_orders_start]

    def render(self) -> None:
        start = DummyOperator(task_id='start')
        if self.full_import:
            self.import_backfill(start=start)
        else:
            self.import_daily(start=start)
