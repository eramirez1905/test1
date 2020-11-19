import unittest
from datetime import datetime
from unittest.mock import MagicMock

from airflow import DAG

from datahub.curated_data.metadata_execution_table import MetadataExecutionTable
from datahub.operators.bigquery.bigquery_update_metadata_execution_operator import BigQueryUpdateMetadataExecutionOperator


class BigQueryUpdateMetadataExecutionOperatorTest(unittest.TestCase):
    def setUp(self):
        self.project_id = 'awesome-project'
        self.dataset_id = 'test_dataset'
        self.table_id = 'test_table'
        self.now = '2020-08-22 20:36:48'
        self.metadata_execution_table = MetadataExecutionTable()
        self.dag = DAG(
            dag_id=f'fake_dag_id',
            start_date=datetime(2019, 2, 25),
            schedule_interval=None,
            max_active_runs=1,
            catchup=False
        )

    def test_update_metadata(self):
        operator = BigQueryUpdateMetadataExecutionOperator(
            dag=self.dag,
            task_id="task_id_fake",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_name=self.table_id,
            metadata_execution_table=self.metadata_execution_table,
        )
        operator._hook = MagicMock()
        cursor_mock = MagicMock()
        operator._hook.get_cursor = MagicMock(return_value=cursor_mock)
        cursor_mock.insert_all = MagicMock()

        operator.get_utc_now = MagicMock(return_value=self.now)

        context = {}
        operator.prepare_template()
        operator.execute(context)

        payload = [
            {
                'json': {
                    'dag_id': self.dag.dag_id,
                    'dataset': self.dataset_id,
                    'table_name': self.table_id,
                    'is_shared': False,
                    'execution_date': '{{execution_date}}',
                    'next_execution_date': '{{next_execution_date}}',
                    'row_count': None,
                    'size_bytes': None,
                    'last_modified_time': None,
                    '_ingested_at': self.now
                }
            }
        ]
        cursor_mock.insert_all.assert_called_once_with(
            self.project_id,
            self.metadata_execution_table.dataset_id,
            self.metadata_execution_table.table_id,
            payload,
            fail_on_error=True,
        )

    def test_update_metadata_for_shared_table(self):
        operator = BigQueryUpdateMetadataExecutionOperator(
            dag=self.dag,
            task_id="task_id_fake",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_name=self.table_id,
            metadata_execution_table=self.metadata_execution_table,
            is_shared=True,
        )
        operator._hook = MagicMock()
        cursor_mock = MagicMock()
        operator._hook.get_cursor = MagicMock(return_value=cursor_mock)
        cursor_mock.insert_all = MagicMock()

        operator.get_utc_now = MagicMock(return_value=self.now)

        context = {}
        operator.prepare_template()
        operator.execute(context)

        payload = [
            {
                'json': {
                    'dag_id': self.dag.dag_id,
                    'dataset': self.dataset_id,
                    'table_name': self.table_id,
                    'is_shared': True,
                    'execution_date': '{{execution_date}}',
                    'next_execution_date': '{{next_execution_date}}',
                    'row_count': None,
                    'size_bytes': None,
                    'last_modified_time': None,
                    '_ingested_at': self.now
                }
            }
        ]
        cursor_mock.insert_all.assert_called_once_with(
            self.project_id,
            self.metadata_execution_table.dataset_id,
            self.metadata_execution_table.table_id,
            payload,
            fail_on_error=True,
        )

    def test_update_backfill_metadata(self):
        backfill_to_date = '2020-01-01 00:00:00'
        operator = BigQueryUpdateMetadataExecutionOperator(
            dag=self.dag,
            task_id="task_id_fake",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_name=self.table_id,
            metadata_execution_table=self.metadata_execution_table,
            backfill_to_date=backfill_to_date
        )
        operator._hook = MagicMock()
        cursor_mock = MagicMock()
        operator._hook.get_cursor = MagicMock(return_value=cursor_mock)
        cursor_mock.insert_all = MagicMock()

        operator.get_utc_now = MagicMock(return_value=self.now)

        context = {}
        operator.prepare_template()
        operator.execute(context)

        payload = [
            {
                'json': {
                    'dag_id': self.dag.dag_id,
                    'dataset': self.dataset_id,
                    'table_name': self.table_id,
                    'is_shared': False,
                    'execution_date': '{{execution_date}}',
                    'next_execution_date': backfill_to_date,
                    'row_count': None,
                    'size_bytes': None,
                    'last_modified_time': None,
                    '_ingested_at': self.now
                }
            }
        ]
        cursor_mock.insert_all.assert_called_once_with(
            self.project_id,
            self.metadata_execution_table.dataset_id,
            self.metadata_execution_table.table_id,
            payload,
            fail_on_error=True,
        )
