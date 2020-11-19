import pytest

from configs.bigquery.tables.intermediate import (
    PandoraBigQueryView,
    LogisticsBigQueryView,
    InvalidPrefix,
)


class TestPandoraBigQueryView:
    def test_validate_does_not_start_with_prefix(self):
        config = PandoraBigQueryView(name="valid_without_correct_prefix")
        with pytest.raises(InvalidPrefix):
            config.validate()


class TestLogisticsBigQueryView:
    def test_validate_does_not_start_with_prefix(self):
        config = LogisticsBigQueryView(name="valid_without_correct_prefix")
        with pytest.raises(InvalidPrefix):
            config.validate()
