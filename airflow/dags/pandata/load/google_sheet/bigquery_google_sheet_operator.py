from typing import List, Optional

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class BigQueryCreateGoogleSheetTableOperator(BaseOperator):
    """
    Creates a BigQuery Google Sheet external table from the
    first sheet. Recreates the table if it exists.
    """

    template_fields = (
        "destination_project_dataset_table",
        "labels",
    )
    ui_color = "#009D5B"

    @apply_defaults
    def __init__(
        self,
        destination_project_dataset_table: str,
        source_uri: str,
        schema_fields: List[dict],
        bigquery_conn_id: str = "bigquery_default",
        delegate_to: Optional[str] = None,
        skip_leading_rows: int = 0,
        labels: Optional[dict] = None,
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)

        self.destination_project_dataset_table = destination_project_dataset_table
        self.source_uri = source_uri
        self.schema_fields = schema_fields
        self.bigquery_conn_id = bigquery_conn_id
        self.skip_leading_rows = skip_leading_rows
        self.delegate_to = delegate_to
        self.src_fmt_configs = (
            {}
            if skip_leading_rows is None
            else {"skipLeadingRows": str(skip_leading_rows)}
        )
        self.labels = labels

    def execute(self, context):
        bq_hook = BigQueryHook(
            bigquery_conn_id=self.bigquery_conn_id, delegate_to=self.delegate_to
        )
        cursor = bq_hook.get_conn().cursor()

        cursor.run_table_delete(
            deletion_dataset_table=self.destination_project_dataset_table,
            ignore_if_missing=True,
        )
        cursor.create_external_table(
            external_project_dataset_table=self.destination_project_dataset_table,
            schema_fields=self.schema_fields,
            source_uris=self.source_uri,
            source_format="GOOGLE_SHEETS",
            src_fmt_configs=self.src_fmt_configs,
            labels=self.labels,
        )
