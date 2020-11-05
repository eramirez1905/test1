import os
from datetime import timedelta

from airflow.contrib.operators.bigquery_operator import BigQueryOperator as BigQueryOperatorBase


class BigQueryOperator(BigQueryOperatorBase):
    template_fields = ('sql', 'destination_dataset_table', 'time_partitioning', 'cluster_fields', 'labels')

    def __init__(self,
                 enabled=True,
                 execution_timeout=timedelta(minutes=15),
                 destination_dataset_table=None,
                 time_partitioning=None,
                 cluster_fields=None,
                 ui_color=None,
                 ui_fgcolor=None,
                 priority='INTERACTIVE',
                 use_legacy_sql=False,
                 *args, **kwargs):
        super().__init__(execution_timeout=execution_timeout,
                         destination_dataset_table=destination_dataset_table,
                         time_partitioning=time_partitioning,
                         cluster_fields=cluster_fields,
                         priority=priority,
                         use_legacy_sql=use_legacy_sql,
                         *args, **kwargs)
        if self.labels is None:
            self.labels = {}

        self.is_enabled = enabled

        self.labels.update(self._get_default_labels())

        self.ui_color = '#1a73e8' if ui_color is None else ui_color
        self.ui_fgcolor = '#FFF' if ui_fgcolor is None else ui_fgcolor

        if not self.is_enabled:
            self.ui_color = '#000'
            self.ui_fgcolor = '#FFF'

    def execute(self, context):
        if not self.is_enabled:
            self.log.info(f'Task "{self.task_id}" is disabled')
            return

        super().execute(context)

    def _get_default_labels(self):
        return {
            'task_id': self._sanitize_bigquery_label(self.task_id),
            'dag_id': self._sanitize_bigquery_label(self.dag_id),
            'business_unit': self._sanitize_bigquery_label(os.getenv('AIRFLOW_BUSINESS_UNIT')),
        }

    def _sanitize_bigquery_label(self, label):
        if len(label) > 63:
            self.log.warning(f"BQ label name {label} exceeds the limit of 63 chars.")

        return label[-63:]
