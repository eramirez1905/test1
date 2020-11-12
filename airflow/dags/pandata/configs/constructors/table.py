from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List, Tuple

from google.cloud.bigquery.schema import SchemaField


class InvalidTimePartitionConfig(BaseException):
    pass


class InvalidTimePartitionType(InvalidTimePartitionConfig):
    pass


class InvalidTimePartitionExpirationMs(InvalidTimePartitionConfig):
    pass


class InvalidField(InvalidTimePartitionConfig):
    pass


class InvalidTableName(BaseException):
    pass


class InvalidTableDescription(BaseException):
    pass


class InvalidSkipLeadingRows(BaseException):
    pass


class InvalidSchemaField(BaseException):
    pass


class InvalidFieldName(BaseException):
    pass


class InvalidFieldDescription(BaseException):
    pass


class InvalidClusterFieldOrder(BaseException):
    pass


class TimePartitioningType(Enum):
    DAY = auto()
    HOUR = auto()

    def __str__(self):
        return self.name


@dataclass(frozen=True)
class ClusterFieldConfig:
    """Clustered in ascending order starting from 1"""

    order: int = 1

    @staticmethod
    def _is_positive_integer(value: int):
        return value > 0

    def validate(self):
        if not self._is_positive_integer(self.order):
            raise InvalidClusterFieldOrder


@dataclass(frozen=True)
class Field:
    name: str
    description: Optional[str] = None
    is_primary_key: bool = False
    cluster: Optional[ClusterFieldConfig] = None
    child_fields: Optional[Tuple[Field, ...]] = None

    def validate(self):
        assert isinstance(self.is_primary_key, bool)

        if self.cluster:
            assert isinstance(self.cluster, ClusterFieldConfig)
            self.cluster.validate()

        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]{1,127}$", self.name):
            raise InvalidFieldName

        if self.description is not None and not re.match(
            r"^.{1,1024}$", self.description
        ):
            raise InvalidFieldDescription

        if self.child_fields is not None:
            for f in self.child_fields:
                assert isinstance(f, self.__class__)
                f.validate()

    def schema_field_api_repr(self) -> dict:
        api_repr = {"name": self.name, "description": self.description}
        if self.child_fields is not None:
            api_repr["fields"] = [f.schema_field_api_repr() for f in self.child_fields]

        return api_repr


@dataclass(frozen=True)
class TimePartitioningConfig:
    partition_type: TimePartitioningType
    field: Optional[str]
    expiration_ms: Optional[int] = None

    def validate(self):
        if not isinstance(self.partition_type, TimePartitioningType):
            raise InvalidTimePartitionType

        if self.expiration_ms is not None and not self._is_positive_integer(
            self.expiration_ms
        ):
            raise InvalidTimePartitionExpirationMs(
                "Value needs to be an integer larger than 0."
            )

        if self.field is not None and not re.match(
            r"^[A-Za-z_][A-Za-z0-9_]{1,127}$", self.field
        ):
            raise InvalidField

    def _is_positive_integer(self, value):
        return value > 0 and isinstance(value, int)

    def generate(self) -> dict:
        time_partitioning = {
            "type": str(self.partition_type),
            "field": self.field,
        }

        if self.expiration_ms:
            time_partitioning["expiration_ms"] = str(self.expiration_ms)

        return time_partitioning


@dataclass(frozen=True)
class BigQueryTable:
    name: str
    description: Optional[str] = None
    time_partitioning_config: Optional[TimePartitioningConfig] = None
    fields: Optional[Tuple[Field, ...]] = None
    is_partition_filter_required: bool = False

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    @property
    def time_partitioning(self):
        return (
            self.time_partitioning_config.generate()
            if self.time_partitioning_config
            else None
        )

    @property
    def cluster_fields(self) -> Optional[List[str]]:
        if self.fields:
            cf = [f for f in self.fields if f.cluster]
            cf = sorted(cf, key=lambda f: f.cluster.order)
            return [f.name for f in cf] if cf else None

    def validate(self):
        assert isinstance(self.is_partition_filter_required, bool)
        if self.time_partitioning_config is not None:
            self.time_partitioning_config.validate()
        if not re.match(r"^[A-Za-z0-9_]{0,1024}$", self.name):
            raise InvalidTableName
        if self.description is not None and not re.match(
            r"^.{0,16384}$", self.description
        ):
            raise InvalidTableDescription


@dataclass(frozen=True)
class RawBigQueryTable(BigQueryTable):
    """Table that is loaded directly from GCS"""

    pass


@dataclass(frozen=True)
class RawDataProcBigQueryTable(BigQueryTable):
    """Table that is loaded with DataProc"""

    pass


@dataclass(frozen=True)
class GoogleSheetTable(BigQueryTable):
    name: str
    source_uri: Optional[str] = None
    schema_fields: Optional[Tuple[SchemaField, ...]] = None
    skip_leading_rows: int = 1
    time_partitioning_config: None = None
    cluster_fields: None = None

    def validate(self):
        if not re.match(r"^[A-Za-z0-9_]{0,1024}$", self.name):
            raise InvalidTableName
        if self.skip_leading_rows < 0:
            raise InvalidSkipLeadingRows(
                f"{self.skip_leading_rows} is not valid. "
                "It has to be greater or equals to 0."
            )
        if not all([isinstance(sf, SchemaField) for sf in self.schema_fields]):
            raise InvalidSchemaField(
                f"{self.schema_fields} is not valid. "
                "Only SchemaField objects can be used."
            )

    @property
    def schema_fields_api_repr(self) -> List[dict]:
        return [sf.to_api_repr() for sf in self.schema_fields]
