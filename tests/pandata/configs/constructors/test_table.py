import pytest

from google.cloud.bigquery.schema import SchemaField

from configs.constructors.table import (
    BigQueryTable,
    ClusterFieldConfig,
    Field,
    GoogleSheetTable,
    InvalidClusterFieldOrder,
    InvalidField,
    InvalidFieldDescription,
    InvalidFieldName,
    InvalidSchemaField,
    InvalidSkipLeadingRows,
    InvalidTableDescription,
    InvalidTableName,
    InvalidTimePartitionExpirationMs,
    InvalidTimePartitionType,
    TimePartitioningConfig,
    TimePartitioningType,
)


class TestField:
    field = Field(name="foo")

    def test_defaults(self):
        assert self.field.description is None
        assert not self.field.is_primary_key
        assert not self.field.cluster

    def test_validate(self):
        self.field.validate()

    @pytest.mark.parametrize("name", ["@", ".", "1", "a" * 129])
    def test_validate_invalid_field_name_exception(self, name):
        field = Field(name=name)
        with pytest.raises(InvalidFieldName):
            field.validate()

    @pytest.mark.parametrize("description", ["a" * 1025, ""])
    def test_validate_invalid_field_description_exception(self, description):
        field = Field(name="foo", description=description)
        with pytest.raises(InvalidFieldDescription):
            field.validate()

    def test_validate_nested_fields(self):
        field = Field(name="foo", child_fields=(Field(name="child"),))
        field.validate()

    @pytest.mark.parametrize(
        "nested_field,exception",
        [
            ("not_a_field", AssertionError),
            (Field(name="@"), InvalidFieldName),
            (Field(name="foo", description=""), InvalidFieldDescription),
            (Field(name="foo", cluster="not_bool"), AssertionError),
            (Field(name="foo", is_primary_key="not_bool"), AssertionError),
        ],
    )
    def test_validate_nested_fields_exception(self, nested_field, exception):
        field = Field(name="foo", child_fields=(nested_field,))
        with pytest.raises(exception):
            field.validate()

    def test_schema_field_api_repr(self):
        nested_field = Field(
            name="foo", child_fields=(Field(name="child", description="i am a child"),)
        )
        expected = {
            "name": "foo",
            "description": None,
            "fields": [{"name": "child", "description": "i am a child"}],
        }
        assert nested_field.schema_field_api_repr() == expected


class TestClusterFieldConfig:
    def test_validate(self):
        config = ClusterFieldConfig(order=1)
        config.validate()

    @pytest.mark.parametrize(
        "value,exception",
        [
            (0, InvalidClusterFieldOrder),
            (-1, InvalidClusterFieldOrder),
            ("s", TypeError),
        ],
    )
    def test_validate_exception(self, value, exception):
        config = ClusterFieldConfig(order=value)
        with pytest.raises(exception):
            config.validate()


class TestTimePartitioningConfig:
    def test_partition_type_day_valid(self):
        config = TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field=None
        )
        config.validate()
        assert config.partition_type == TimePartitioningType.DAY

    def test_partition_type_hour_valid(self):
        config = TimePartitioningConfig(
            partition_type=TimePartitioningType.HOUR, field=None
        )
        config.validate()
        assert config.partition_type == TimePartitioningType.HOUR

    def test_partition_type_day_invalid_type(self):
        config = TimePartitioningConfig(partition_type="weird_value", field=None)
        with pytest.raises(InvalidTimePartitionType):
            config.validate()

    @pytest.mark.parametrize("expiration_ms", [0, -1, -100, 2.1])
    def test_expiration_ms_invalid_integer(self, expiration_ms):
        config = TimePartitioningConfig(
            partition_type=TimePartitioningType.HOUR,
            field=None,
            expiration_ms=expiration_ms,
        )
        with pytest.raises(InvalidTimePartitionExpirationMs):
            config.validate()

    def test_expiration_ms_invalid_string(self):
        config = TimePartitioningConfig(
            partition_type=TimePartitioningType.HOUR, field=None, expiration_ms="a",
        )
        with pytest.raises(TypeError):
            config.validate()

    @pytest.mark.parametrize("field", ["*", "@", "_", "_*", "a" * 129])
    def test_field_invalid(self, field):
        config = TimePartitioningConfig(
            partition_type=TimePartitioningType.HOUR, field=field
        )
        with pytest.raises(InvalidField):
            config.validate()

    @pytest.mark.parametrize(
        "value, expected",
        [(0, False), (-1, False), (-100, False), (2.1, False), (1, True), (2, True)],
    )
    def test__is_positive_integer(self, value, expected):
        assert (
            TimePartitioningConfig(
                partition_type=TimePartitioningType.DAY, field=None
            )._is_positive_integer(value)
            == expected
        )


class TestBigQueryTable:
    table_config = BigQueryTable(name="foobar")

    def test_name(self):
        assert self.table_config.name == "foobar"

    @pytest.mark.parametrize(
        "config,expected",
        [
            (table_config, None),
            (BigQueryTable(name="foobar", fields=(Field(name="fiz"),)), None),
            (
                BigQueryTable(
                    name="foobar",
                    fields=(
                        Field(name="fiz"),
                        Field(name="bar", cluster=ClusterFieldConfig(order=1)),
                    ),
                ),
                ["bar"],
            ),
            (
                BigQueryTable(
                    name="foobar",
                    fields=(
                        Field(name="fiz"),
                        Field(name="bar", cluster=ClusterFieldConfig(order=2)),
                        Field(name="buz", cluster=ClusterFieldConfig(order=1)),
                    ),
                ),
                ["buz", "bar"],
            ),
        ],
    )
    def test_cluster_fields(self, config, expected):
        assert config.cluster_fields == expected

    def test_time_partitioning_is_none(self):
        assert self.table_config.time_partitioning is None

    def test_is_partition_filter_required_is_false(self):
        assert not self.table_config.is_partition_filter_required

    def test_time_partitioning(self):
        table_config = BigQueryTable(
            name="foobar",
            time_partitioning_config=TimePartitioningConfig(
                partition_type=TimePartitioningType.DAY, field=None, expiration_ms=10000
            ),
        )
        expected = {
            "field": None,
            "type": "DAY",
            "expiration_ms": "10000",
        }

        assert table_config.time_partitioning == expected

    @pytest.mark.parametrize("table_name", ["*", "@", "_*", "a" * 1025])
    def test_name_invalid(self, table_name):
        table_config = BigQueryTable(name=table_name)
        with pytest.raises(InvalidTableName):
            table_config.validate()

    def test_description_invalid(self):
        table_config = BigQueryTable(name="foo", description="a" * 16835)
        with pytest.raises(InvalidTableDescription):
            table_config.validate()

    @pytest.mark.parametrize(
        "time_partitioning_config,exception",
        [
            (
                TimePartitioningConfig(
                    partition_type="blah", field=None, expiration_ms=10000
                ),
                InvalidTimePartitionType,
            ),
            (
                TimePartitioningConfig(
                    partition_type=TimePartitioningType.DAY,
                    field="_",
                    expiration_ms=10000,
                ),
                InvalidField,
            ),
            (
                TimePartitioningConfig(
                    partition_type=TimePartitioningType.DAY,
                    field=None,
                    expiration_ms=0,
                ),
                InvalidTimePartitionExpirationMs,
            ),
        ],
    )
    def test_time_partitioning_invalid(self, time_partitioning_config, exception):
        table_config = BigQueryTable(
            name="foobar", time_partitioning_config=time_partitioning_config
        )
        with pytest.raises(exception):
            table_config.validate()

    def test_is_partition_filter_required_invalid(self):
        table_config = BigQueryTable(name="foobar", is_partition_filter_required="f")
        with pytest.raises(AssertionError):
            table_config.validate()


class TestGoogleSheetTable:
    table_config = GoogleSheetTable(
        name="foobar",
        source_uri="https://foo",
        schema_fields=(
            SchemaField(name="vendor_code", field_type="STRING", mode="NULLABLE"),
        ),
    )

    def test_name(self):
        assert self.table_config.name == "foobar"

    def test_source_uri(self):
        assert self.table_config.source_uri == "https://foo"

    def test_schema_fields_api_repr(self):
        expected = [
            {
                "description": None,
                "mode": "NULLABLE",
                "name": "vendor_code",
                "type": "STRING",
            },
        ]
        assert self.table_config.schema_fields_api_repr == expected

    def test_time_partitioning(self):
        assert self.table_config.time_partitioning is None

    def test_cluster_fields(self):
        assert self.table_config.cluster_fields is None

    def test_skip_leading_rows_default(self):
        assert self.table_config.skip_leading_rows == 1

    def test_skip_leading_rows_invalid(self):
        with pytest.raises(InvalidSkipLeadingRows):
            GoogleSheetTable(name="foobar", skip_leading_rows=-1).validate()

    def test_schema_fields_invalid(self):
        with pytest.raises(InvalidSchemaField):
            GoogleSheetTable(
                name="foobar",
                schema_fields=(
                    SchemaField(
                        name="vendor_code", field_type="STRING", mode="NULLABLE"
                    ),
                    "not SchemaField",
                ),
            ).validate()

    @pytest.mark.parametrize("table_name", ["*", "@", "_*", "a" * 1025])
    def test_name_invalid(self, table_name):
        table_config = GoogleSheetTable(name=table_name)
        with pytest.raises(InvalidTableName):
            table_config.validate()
