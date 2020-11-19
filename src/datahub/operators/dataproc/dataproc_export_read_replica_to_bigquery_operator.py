from datetime import datetime

from datahub.common.helpers import TableSettings
from datahub.operators.dataproc.base_dataproc_job_operator import BaseDataprocJobOperator


class DataprocExportReadReplicaToBigQueryOperator(BaseDataprocJobOperator):

    def __init__(self, config, dataset: str, table: TableSettings = None, backfill: bool = False,
                 backfill_from_date: datetime = None, backfill_to_date: datetime = None,
                 countries=None, regions=None, offset_minutes=360, **kwargs):
        self.config = config
        self.backfill = backfill
        self.backfill_from_date = backfill_from_date
        self.backfill_to_date = backfill_to_date
        self.countries = countries
        self.regions = regions
        self.dataset = dataset
        self.staging_table = table.generate_staging_table_template(f"{table.table_name}")
        self.offset_minutes = offset_minutes

        pool = table.pool_name if self.backfill is False else self.config['pools']['export_cloud_sql_backfill']['name']
        super().__init__(config=config, import_job_type='export_cloud_sql', table=table, pool=pool, **kwargs)

    @property
    def job_params(self):
        params = [
            '--source-app', self.table.database.name,
            '--source-jdbc-database-type', self.table.database.type.name,
            '--source-table', self.table.name,
            '--staging-table', self.staging_table,
            '--big-query-project-id', self.config.get('bigquery').get('project_id'),
            '--big-query-bucket-name', self.config.get('bigquery').get('bucket_name'),
            '--big-query-dataset', self.dataset,
            '--created-at-column', self.table.created_at_column,
            '--updated-at-column', self.table.updated_at_column,
            '--execution-date', "{{ execution_date.strftime('%Y-%m-%d %H:%M:%S') }}",
            '--file-format', self.table.file_format,
            '--business-unit', self.config.get('business-unit').get('name'),
        ]

        if self.database.is_regional and self.regions is not None:
            for col in self.regions:
                params.extend(['--region', col])
        if not self.database.is_regional and self.countries is not None:
            for col in self.countries:
                params.extend(['--country-code', col])

        for col in self.table.pk_columns:
            params.extend(['--source-unique-columns', col])

        # timestamps should be converted to a format accepted by java.sql.Timestamp#valueOf
        # it's safe to remove the TZ info because the dates are in UTC
        to_date = "{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S') }}"
        if self.backfill:
            params.extend(['--job-timeout', f'{60 * 24 * 3}'])  # minutes * hours * days
            params.extend(['--parallel-jobs', '2'])
            if self.table.filter_import_by_date:
                from_date = datetime(year=2015, month=1, day=1).strftime('%Y-%m-%d %H:%M:%S')
                to_date = "{{ execution_date.strftime('%Y-%m-%d %H:%M:%S') }}"

                if self.backfill_from_date:
                    from_date = self.backfill_from_date.strftime('%Y-%m-%d %H:%M:%S')
                if self.backfill_to_date:
                    to_date = self.backfill_to_date.strftime('%Y-%m-%d %H:%M:%S')

                params.extend([
                    '--from-date', from_date,
                    '--to-date', to_date,
                    '--use-jdbc-partition-column',
                ])
        else:
            if self.table.filter_import_by_date:
                from_date = "{{{{ (execution_date - macros.timedelta(minutes={})).strftime('%Y-%m-%d %H:%M:%S') }}}}"
                to_date = "{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S') }}"
                params.extend([
                    '--from-date', from_date.format(self.offset_minutes),
                    '--to-date', to_date,
                ])

        params.extend([
            '--replica-lag-upper-boundary', to_date,
        ])
        if self.table.namespace is not None:
            params.extend(['--namespace', self.table.namespace])

        return params

    def execute(self, context):
        if self.backfill:
            if self.backfill_to_date > datetime.now():
                raise ValueError(f'Backfill to_date is in the future. {self.backfill_to_date}')
            if not self.database.is_regional and self.countries is not None:
                self.log.info("Backfill countries={}".format(','.join(self.countries)))
                super().execute(context)
                return
            elif self.database.is_regional and self.regions is not None:
                self.log.info("Backfill regions={}".format(','.join(self.regions)))
                super().execute(context)
                return

            self.log.info("Backfill disabled")
        else:
            super().execute(context)
