from datetime import timedelta

from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator as BigQueryToBigQueryOperatorBase


class BigQueryToBigQueryOperator(BigQueryToBigQueryOperatorBase):
    ui_color = '#36bb2b'
    ui_fgcolor = '#FFF'

    def __init__(self,
                 enabled=True,
                 execution_timeout=timedelta(minutes=15),
                 labels=None,
                 ui_color=None,
                 ui_fgcolor=None,
                 *args, **kwargs):
        super().__init__(execution_timeout=execution_timeout,
                         labels=labels,
                         *args, **kwargs)
        if self.labels is None:
            self.labels = {}

        self.is_enabled = enabled

        labels = {
            'dag_id': self.dag.dag_id,
            'task_id': self.task_id,
        }
        self.labels = {**self.labels, **labels}

        self.ui_color = '#36bb2b' if ui_color is None else ui_color
        self.ui_fgcolor = '#FFF' if ui_fgcolor is None else ui_fgcolor

        if not self.is_enabled:
            self.ui_color = '#000'
            self.ui_fgcolor = '#FFF'

    def execute(self, context):
        if not self.is_enabled:
            self.log.info(f'Task "{self.task_id}" is disabled')
            return

        super().execute(context)
