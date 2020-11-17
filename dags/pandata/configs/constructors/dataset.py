import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional, Tuple

from configs.constructors.table import BigQueryTable


class InvalidDatasetId(BaseException):
    pass


class DatasetConfigNotFound(BaseException):
    pass


class DuplicateTables(BaseException):
    pass


ONE_WEEK_IN_MS = round(timedelta(days=7).total_seconds() * 1000)


@dataclass(frozen=True)
class AuthorizedView:
    project_id: str
    dataset_id: str
    table_id: str


@dataclass(frozen=True)
class UserAccess:
    user_emails: Optional[Tuple[str]] = None
    group_emails: Optional[Tuple[str]] = None


@dataclass(frozen=True)
class DatasetAccess:
    write: UserAccess = UserAccess()
    read: UserAccess = UserAccess()
    views: Optional[Tuple[AuthorizedView]] = None


@dataclass(frozen=True)
class BigQueryDataset:
    name: str
    description: Optional[str] = None
    friendly_name: Optional[str] = None
    default_table_expiration_ms: Optional[int] = None
    default_partition_expiration_ms: Optional[int] = None
    access: Optional[DatasetAccess] = None
    tables: Optional[Tuple[BigQueryTable, ...]] = None

    def validate(self):
        is_valid = re.match(r"^[A-Za-z0-9_]{0,1024}$", self.name)

        if not is_valid:
            raise InvalidDatasetId

        if self.tables:
            if len(set(self.tables)) != len(self.tables):
                raise DuplicateTables

            for t in self.tables:
                t.validate()

    @property
    def properties(self) -> dict:
        # See https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets
        resource = {}
        if self.description:
            resource["description"] = self.description

        if self.friendly_name:
            resource["friendlyName"] = self.friendly_name

        if self.default_table_expiration_ms:
            resource["defaultTableExpirationMs"] = str(self.default_table_expiration_ms)

        if self.default_partition_expiration_ms:
            resource["defaultPartitionExpirationMs"] = str(
                self.default_partition_expiration_ms
            )

        return resource


class TemporaryBigQueryDataset(BigQueryDataset):
    """Dataset with temporary tables"""

    default_table_expiration_ms = ONE_WEEK_IN_MS


class ExternalBigQueryDataset(BigQueryDataset):
    """Dataset that we do not manage access for"""

    access = None


@dataclass(frozen=True)
class BigQueryConfig:
    datasets: Tuple[BigQueryDataset, ...]

    def get(self, name: str) -> BigQueryDataset:
        for d in self.datasets:
            if d.name == name:
                return d

        raise DatasetConfigNotFound

    def validate(self):
        for d in self.datasets:
            d.validate()
