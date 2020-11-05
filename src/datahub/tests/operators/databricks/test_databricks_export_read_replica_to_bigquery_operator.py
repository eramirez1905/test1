import unittest
from datetime import datetime

from airflow import DAG

from configuration import config
from datahub.common.helpers import DatabaseSettings, TableSettings
from datahub.operators.databricks.databricks_export_read_replica_to_bigquery_operator import \
    DatabricksExportReadReplicaToBigQueryOperator


class CuratedDataBigQueryOperatorTest(unittest.TestCase):
    def test_database_enabled(self):
        operator = self._get_export_read_replica_operator('test_enabled', 'test')
        self.assertEqual(operator.is_enabled, True)

    def test_table_disabled(self):
        operator = self._get_export_read_replica_operator('test_table_disabled', 'test')
        self.assertEqual(operator.is_enabled, False)

    def test_database_disabled(self):
        operator = self._get_export_read_replica_operator('test_database_disabled', 'test')
        self.assertEqual(operator.is_enabled, False)

    @staticmethod
    def _get_table(database_name, table_name):
        for current_database_name, params in config['dwh_merge_layer_databases'].items():
            if current_database_name == database_name:
                database = DatabaseSettings(current_database_name, params)
                for table in database.tables:
                    if table.name == table_name:
                        return table
        return None

    def _get_export_read_replica_operator(self, database_name, table_name):
        dag = DAG(
            dag_id=f'test_dag',
            start_date=datetime(2019, 2, 25),
            schedule_interval=None)

        table = self._get_table(database_name, table_name)
        self.assertIsInstance(table, TableSettings)

        return DatabricksExportReadReplicaToBigQueryOperator(
            dag=dag,
            config=config,
            task_id=f'import-{table.database.name}_{table.name}',
            table=table,
            dataset='staging',
        )
