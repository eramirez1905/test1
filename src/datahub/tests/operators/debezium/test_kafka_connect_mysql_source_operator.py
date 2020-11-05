import json
import unittest
from datetime import datetime

from airflow import DAG

from configuration import config
from datahub.common.helpers import TableSettings
from datahub.dwh_import_read_replica import DwhImportReadReplica
from datahub.operators.debezium.kafka_connect_mysql_source_operator import KafkaConnectMysqlSourceOperator


class KafkaConnectMysqlSourceOperatorTest(unittest.TestCase):
    def test_prepare_template(self):
        operator = self._get_export_read_replica_operator('airflow_test', 'ab_permission')
        expected = {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "snapshot.mode": "when_needed",
            "database.hostname": "rds.eu.airflow.staging-eu.tf",
            "database.port": 3306,
            "database.user": "foodora",
            "database.password": "",
            "database.server.name": "airflow_test",
            "database.include.list": "airflow_test_.*",
            "database.history.kafka.bootstrap.servers": "broker_endpoints",
            "database.history.kafka.topic": "schema_history_airflow_test",
            "include.schema.changes": "true",
            "table.include.list": "airflow_test_([a-z_]+)\\.ab_permission",
            "binary.handling.mode": "base64",
            "transforms": "Reroute,unwrap,ingestedAt,createdDate,TimestampConverterCreatedDate,shardingColumnFake",
            "transforms.Reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
            "transforms.Reroute.topic.regex": "(.*)\\.airflow_test_(.*)\\.(.*)",
            "transforms.Reroute.topic.replacement": "$1_$3",
            "transforms.Reroute.key.field.name": "business_unit",
            "transforms.Reroute.key.field.regex": "(.*)\\.airflow_test_(.+)\\.(.*)",
            "transforms.Reroute.key.field.replacement": "$2",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.add.fields": "op,table,source.ts_ms",
            "transforms.ingestedAt.type": "org.apache.kafka.connect.transforms.InsertField$Value",
            "transforms.ingestedAt.timestamp.field": "_ingested_at",
            "transforms.createdDate.type": "org.apache.kafka.connect.transforms.InsertField$Value",
            "transforms.createdDate.timestamp.field": "created_date",
            "transforms.TimestampConverterCreatedDate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.TimestampConverterCreatedDate.field": "created_date",
            "transforms.TimestampConverterCreatedDate.target.type": "Date",
            "transforms.shardingColumnFake.type": "org.apache.kafka.connect.transforms.InsertField$Value",
            "transforms.shardingColumnFake.static.field": "business_unit",
            "transforms.shardingColumnFake.static.value": "",
            "producer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor"
        }
        operator.prepare_template()

        self.assertEqual(operator.payload, json.dumps(expected, indent=2))

    def test_prepare_template_custom_sharding_column_table(self):
        operator = self._get_export_read_replica_operator('airflow_custom_sharding_column', 'custom_sharding_column_table')
        expected = {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "snapshot.mode": "when_needed",
            "database.hostname": "rds.eu.airflow.staging-eu.tf",
            "database.port": 3306,
            "database.user": "foodora",
            "database.password": "",
            "database.server.name": "airflow_custom_sharding_column",
            "database.include.list": "airflow_custom_sharding_column_.*",
            "database.history.kafka.bootstrap.servers": "broker_endpoints",
            "database.history.kafka.topic": "schema_history_airflow_custom_sharding_column",
            "include.schema.changes": "true",
            "table.include.list": "airflow_custom_sharding_column_([a-z_]+)\\.custom_sharding_column_table",
            "binary.handling.mode": "base64",
            "transforms": "Reroute,unwrap,ingestedAt,createdDate,TimestampConverterCreatedDate,shardingColumnFake",
            "transforms.Reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
            "transforms.Reroute.topic.regex": "(.*)\\.airflow_custom_sharding_column_(.*)\\.(.*)",
            "transforms.Reroute.topic.replacement": "$1_$3",
            "transforms.Reroute.key.field.name": "custom_sharding_column",
            "transforms.Reroute.key.field.regex": "(.*)\\.airflow_custom_sharding_column_(.+)\\.(.*)",
            "transforms.Reroute.key.field.replacement": "$2",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.add.fields": "op,table,source.ts_ms",
            "transforms.ingestedAt.type": "org.apache.kafka.connect.transforms.InsertField$Value",
            "transforms.ingestedAt.timestamp.field": "_ingested_at",
            "transforms.createdDate.type": "org.apache.kafka.connect.transforms.InsertField$Value",
            "transforms.createdDate.timestamp.field": "created_date",
            "transforms.TimestampConverterCreatedDate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.TimestampConverterCreatedDate.field": "created_date",
            "transforms.TimestampConverterCreatedDate.target.type": "Date",
            "transforms.shardingColumnFake.type": "org.apache.kafka.connect.transforms.InsertField$Value",
            "transforms.shardingColumnFake.static.field": "custom_sharding_column",
            "transforms.shardingColumnFake.static.value": "",
            "producer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor"
        }
        operator.prepare_template()

        self.assertEqual(operator.payload, json.dumps(expected, indent=2))

    @staticmethod
    def _get_table(database_name, table_name):
        dwh_import = DwhImportReadReplica(config)
        return [t for t in dwh_import.databases.get(database_name).tables if t.name == table_name][0]

    def _get_export_read_replica_operator(self, database_name, table_name):
        dag = DAG(
            dag_id=f'test_dag',
            start_date=datetime(2019, 2, 25),
            schedule_interval=None
        )

        table = self._get_table(database_name, table_name)
        self.assertIsInstance(table, TableSettings)

        return KafkaConnectMysqlSourceOperator(
            dag=dag,
            task_id=f'import-{table.database.name}_{table.name}',
            database=table.database,
            project_id='project_id',
            dataset_id='staging',
            broker_endpoints='broker_endpoints',
        )
