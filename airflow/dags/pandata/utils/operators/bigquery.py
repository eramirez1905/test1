from typing import Tuple, Optional

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from configs.constructors.table import Field
from utils.hooks.bigquery import BigQueryPatchTableHook


@apply_defaults
class BigQueryPatchTableOperator(BaseOperator):
    """
    Patches table and updates schema field descriptions.
    """

    template_fields = ("dataset_id", "table_id", "project_id", "labels")

    def __init__(
        self,
        gcp_conn_id: str,
        project_id: str,
        dataset_id: str,
        table_id: str,
        description: Optional[str] = None,
        fields: Optional[Tuple[Field, ...]] = None,
        labels: Optional[dict] = None,
        delegate_to: Optional[str] = None,
        is_partition_filter_required: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.description = description
        self.fields = fields
        self.labels = labels
        self.delegate_to = delegate_to
        self.is_partition_filter_required = is_partition_filter_required

    def execute(self, context):
        if self.description is None and self.fields is None:
            self.log.info("Doing nothing since nothing to patch.")
            return

        hook = BigQueryPatchTableHook(
            bigquery_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )

        cursor = hook.get_conn().cursor()
        self.log.info(
            f"Start patching table: {self.project_id}.{self.dataset_id}.{self.table_id}"
        )
        schema = hook.get_schema(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
        )
        patched_schema = hook.patch_schema(fields=self.fields, schema=schema)

        cursor.patch_table(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            description=self.description,
            require_partition_filter=(
                self.is_partition_filter_required
                if self.is_partition_filter_required
                else None
            ),
            schema=patched_schema["fields"],
        )
