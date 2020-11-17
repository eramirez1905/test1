import json
import os

from datahub.operators.debezium.kafka_connect_base_operator import KafkaConnectBaseOperator


class KafkaConnectMysqlSourceOperator(KafkaConnectBaseOperator):
    template_fields = ['source_name', 'payload']
    ui_color: str = '#59c4bc'
    ui_fgcolor: str = '#fff'

    def __init__(self, broker_endpoints, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker_endpoints = broker_endpoints
        self.source_name = f"mysql.{self.database.name}"

    def execute(self, context):
        self._update_connect_config(self.source_name, self.read_replica_password)

    def prepare_template(self):
        tables = [f"{self.database.name}_([a-z_]+)\\.{table.name}" for table in self.database.tables]
        table_include_list = ",".join(tables)
        self.payload = json.dumps({
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "snapshot.mode": "when_needed",
            "database.hostname": self.database.endpoint,
            "database.port": self.database.port,
            "database.user": self.read_replica_username,
            "database.password": "",
            "database.server.name": self.database.name,
            "database.include.list": f"{self.database.name}_.*",
            "database.history.kafka.bootstrap.servers": self.broker_endpoints,
            "database.history.kafka.topic": f"schema_history_{self.database.name}",
            "include.schema.changes": "true",
            "table.include.list": table_include_list,
            "binary.handling.mode": "base64",

            "transforms": "Reroute,unwrap,ingestedAt,createdDate,TimestampConverterCreatedDate,shardingColumnFake",

            "transforms.Reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
            "transforms.Reroute.topic.regex": f"(.*)\\.{self.database.name}_(.*)\\.(.*)",
            "transforms.Reroute.topic.replacement": "$1_$3",
            "transforms.Reroute.key.field.name": self.database.sharding_column,
            "transforms.Reroute.key.field.regex": f"(.*)\\.{self.database.name}_(.+)\\.(.*)",
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
            "transforms.shardingColumnFake.static.field": self.database.sharding_column,
            "transforms.shardingColumnFake.static.value": "",

            "producer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor"
        }, indent=2)

    @property
    def read_replica_username(self):
        return self.database.credentials.get('username', '')

    @property
    def read_replica_password(self):
        return os.getenv(self.database.credentials.get('password', ''), '')
