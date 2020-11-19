import pytest

from configs.constructors.table import BigQueryTable, InvalidTableName
from configs.constructors.dataset import (
    BigQueryConfig,
    BigQueryDataset,
    DatasetConfigNotFound,
    DuplicateTables,
    InvalidDatasetId,
)


class TestBigQueryDataset:
    @pytest.mark.parametrize("name", ["foo@", "a-b-c", "1&b&c", "a" * 1025])
    def test_validate_invalid_dataset_id(self, name):
        invalid_dataset = BigQueryDataset(name=name)

        with pytest.raises(InvalidDatasetId):
            invalid_dataset.validate()

    @pytest.mark.parametrize(
        "tables",
        [
            (BigQueryTable(name="same"), BigQueryTable(name="same")),
            (BigQueryTable(name="same"), BigQueryTable(name="same", description="foo")),
        ],
    )
    def test_validate_duplicate_tables(self, tables):
        invalid_dataset = BigQueryDataset(name="foo", tables=tables)

        with pytest.raises(DuplicateTables):
            invalid_dataset.validate()

    @pytest.mark.parametrize("name", ["foo", "f" * 1024])
    def test_validate_no_error(self, name):
        valid_dataset = BigQueryDataset(name=name)
        valid_dataset.validate()

    def test_properties_empty(self):
        dataset = BigQueryDataset(name="dataset")
        assert dataset.properties == {}

    def test_properties_friendly_name(self):
        dataset = BigQueryDataset(name="dataset", friendly_name="foobar")
        assert dataset.properties == {"friendlyName": "foobar"}

    def test_properties_description(self):
        dataset = BigQueryDataset(name="dataset", description="foo")
        assert dataset.properties == {"description": "foo"}

    def test_properties_default_table_expiration_ms(self):
        dataset = BigQueryDataset(name="dataset", default_table_expiration_ms=1000)
        assert dataset.properties == {"defaultTableExpirationMs": "1000"}

    def test_properties_default_partition_expiration_ms(self):
        dataset = BigQueryDataset(name="dataset", default_partition_expiration_ms=1000)
        assert dataset.properties == {"defaultPartitionExpirationMs": "1000"}


class TestBigQueryConfig:
    DATASET_ID = "dataset"
    dataset_config = BigQueryDataset(name=DATASET_ID)
    bq_config = BigQueryConfig(datasets=(dataset_config,))

    def test_get(self):
        assert self.bq_config.get(name=self.DATASET_ID) == self.dataset_config

    def test_get_dataset_not_found_exception(self):
        with pytest.raises(DatasetConfigNotFound):
            assert self.bq_config.get(name="missing_dataset") == self.dataset_config

    def test_validate_success(self):
        self.bq_config.validate()

    @pytest.mark.parametrize(
        "datasets,exception",
        [
            (
                BigQueryConfig(datasets=(BigQueryDataset(name="@error"),)),
                InvalidDatasetId,
            ),
            (
                BigQueryConfig(
                    datasets=(
                        BigQueryDataset(name="ok", tables=(BigQueryTable(name="*"),)),
                    )
                ),
                InvalidTableName,
            ),
        ],
    )
    def test_validate_error(self, datasets, exception):
        with pytest.raises(exception):
            datasets.validate()
