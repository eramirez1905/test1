from datahub.common.helpers import TableSettings
from datahub.operators.dataproc.base_dataproc_create_cluster_operator import BaseDataprocCreateClusterOperator


class DataprocCreateClusterOperator(BaseDataprocCreateClusterOperator):

    def __init__(self, cluster_name, config, table: TableSettings = None, backfill: bool = False, **kwargs):
        self.config = config
        self.backfill = backfill
        self.cluster_name = cluster_name
        super().__init__(cluster_name=cluster_name, config=config, import_job_type='export_cloud_sql', table=table,
                         auto_delete_ttl=60 * 60 * 2 if not self.backfill else 60 * 60 * 24,  # 2 hours for normal job and 24 hours for backfill
                         **kwargs)

    def execute(self, context):
        super().execute(context)

    @property
    def spark_cluster(self):
        return {**super().spark_cluster, **self.backfill_spark_cluster}

    @property
    def backfill_spark_cluster(self):
        export_cloud_sql_backfill = {}
        if self.backfill:
            default = self.config.get('dataproc').get('export_cloud_sql_backfill', {}).get('default', {}).get(
                'spark_cluster', {})
            table_default = self.table.spark_cluster.get('export_cloud_sql_backfill',
                                                         self.table.spark_cluster.get('default', {}))
            export_cloud_sql_backfill = {
                **default,
                **table_default
            }

        return export_cloud_sql_backfill
