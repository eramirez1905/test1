import json

from airflow import AirflowException

from datahub.common.helpers import TableSettings
from datahub.operators.debezium.kafka_connect_base_operator import KafkaConnectBaseOperator


class KafkaConnectBigQuerySinkOperator(KafkaConnectBaseOperator):
    template_fields = ['sink_name', 'payload']
    ui_color: str = '#59c4bc'
    ui_fgcolor: str = '#fff'

    def __init__(self, table: TableSettings, schema_registry_location, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_registry_location = schema_registry_location
        self.table = table
        self.sink_name = f"bigquery.{self.table.table_name}"

    def execute(self, context):
        if self.table.time_partitioning != "created_date":
            raise AirflowException(f"Table {self.table.table_name} has not allowed time_partitioning column {self.table.time_partitioning}. "
                                   f"It must be created_date")
        self._update_connect_config(self.sink_name)

    def prepare_template(self):
        self.payload = json.dumps({
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "tasks.max": "3",
            "topics": f"{self.table.table_name}",
            "sanitizeTopics": "true",
            "autoCreateTables": "true",
            "autoUpdateSchemas": "true",
            "schemaRetriever": "com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever.SchemaRegistrySchemaRetriever",
            "schemaRegistryLocation": self.schema_registry_location,
            "bufferSize": "100000",
            "maxWriteSize": "10000",
            "tableWriteWait": "1000",
            "project": f"{self.project_id}",
            "datasets": f".*={self.dataset_id}",
            "bigQueryPartitionDecorator": False,
            "timestampPartitionFieldName": self.table.time_partitioning,
            "clusteringPartitionFieldNames": ",".join(self.table.pk_columns),
            "allBQFieldsNullable": True,
            "kafkaKeyFieldName": "_keys",
            "kafkaDataFieldName": "_metadata",

            "transforms": "blacklistIngestedAt,insertIngestedAt,insertTsTimePartitioning,TimestampConverterCreatedDate,shardingColumn",
            "transforms.blacklistIngestedAt.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.blacklistIngestedAt.blacklist": f"_ingested_at,{self.table.time_partitioning},{self.table.sharding_column}",
            "transforms.insertIngestedAt.type": "org.apache.kafka.connect.transforms.InsertField$Value",
            "transforms.insertIngestedAt.timestamp.field": "_ingested_at",

            "transforms.insertTsTimePartitioning.type": "org.apache.kafka.connect.transforms.InsertField$Value",
            "transforms.insertTsTimePartitioning.timestamp.field": self.table.time_partitioning,
            "transforms.TimestampConverterCreatedDate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.TimestampConverterCreatedDate.field": self.table.time_partitioning,
            "transforms.TimestampConverterCreatedDate.target.type": "Date",

            "transforms.shardingColumn.type": "com.deliveryhero.kafka.connect.transforms.KeyToValue",
            "transforms.shardingColumn.fields": self.table.sharding_column,

            "keyfile": "/etc/secrets/google_private_key",
            "consumer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor"
        }, indent=2)
