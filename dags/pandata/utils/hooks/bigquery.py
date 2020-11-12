from typing import Tuple, List, Optional

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.exceptions import AirflowException

from configs.constructors.table import Field


class SchemaFieldMissing(AirflowException):
    pass


class BigQueryPatchTableHook(BigQueryHook):
    def get_schema(self, project_id: str, dataset_id: str, table_id: str) -> dict:
        """
        Get the schema for a given table but using the project id defined
        in the hook rather than the connection

        :param project_id: the project ID of the requested table
        :param dataset_id: the dataset ID of the requested table
        :param table_id: the table ID of the requested table
        :return: a table schema
        """
        tables_resource = (
            self.get_service()
            .tables()
            .get(projectId=project_id, datasetId=dataset_id, tableId=table_id)
            .execute(num_retries=self.num_retries)
        )
        return tables_resource["schema"]

    def patch_schema(self, fields: Optional[Tuple[Field, ...]], schema: dict) -> dict:
        schema_fields = schema["fields"]
        config_fields = [f.schema_field_api_repr() for f in fields]
        self.merge_schema_fields_api_repr(original=schema_fields, update=config_fields)
        return schema

    def merge_schema_fields_api_repr(self, original: List[dict], update: List[dict]):
        for sf_new in update:
            sf_old = self.__get_schema_field(original, sf_new["name"])
            self.__update_description(
                schema_field=sf_old, description=sf_new.get("description")
            )

            if sf_new.get("fields"):
                self.merge_schema_fields_api_repr(
                    original=sf_old["fields"], update=sf_new["fields"]
                )

    @staticmethod
    def __get_schema_field(schema_fields: List[dict], name: str) -> dict:
        sf = [f for f in schema_fields if f["name"] == name]
        try:
            return sf[0]
        except IndexError:
            raise SchemaFieldMissing(f"Schema field with name '{name}' does not exist.")

    @staticmethod
    def __update_description(schema_field: dict, description: Optional[str]):
        if description is not None:
            schema_field["description"] = description
