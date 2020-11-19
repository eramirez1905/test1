from datahub.common.helpers import TableSettings
from datahub.operators.dataproc.base_dataproc_delete_cluster_operator import BaseDataprocDeleteClusterOperator


class DataprocDeleteClusterOperator(BaseDataprocDeleteClusterOperator):

    def __init__(self, cluster_name, config, table: TableSettings = None, **kwargs):
        super().__init__(cluster_name=cluster_name, config=config, import_job_type='export_cloud_sql', table=table,
                         **kwargs)

    def execute(self, context):
        super().execute(context)
