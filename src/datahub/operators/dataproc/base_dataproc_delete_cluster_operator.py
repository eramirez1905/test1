from abc import ABCMeta

from airflow import AirflowException
from airflow.contrib.operators.dataproc_operator import DataprocClusterDeleteOperator

from datahub.common.helpers import TableSettings
from googleapiclient.errors import HttpError


class BaseDataprocDeleteClusterOperator(DataprocClusterDeleteOperator, metaclass=ABCMeta):
    ui_color = '#9370DB'

    def __init__(self, cluster_name, config, import_job_type, table: TableSettings = None, **kwargs):
        if import_job_type not in config['dataproc']:
            raise AirflowException(f"Missing config for Dataproc job {import_job_type}")

        self.database = table.database
        self.config = config
        self.table = table
        self._job_type = import_job_type
        self._job_config = config['dataproc'][import_job_type]

        if not self.is_enabled:
            self.ui_color = '#000000'
            self.ui_fgcolor = '#FFF'

        super().__init__(project_id=config['dataproc']['project_id'],
                         region=config['dataproc'].get('region', 'europe-west1'),
                         cluster_name=cluster_name,
                         **kwargs
                         )

    def execute(self, context):
        if not self.is_enabled:
            self.log.info(f'Job "{self.import_job_type}" is disabled in config')
            return

        try:
            super().execute(context)
        except HttpError as e:
            # Check if cluster already deleted, if it is, make task successful
            if e.resp.status == 404:
                return True
            else:
                raise e
        except Exception as e:
            raise e

    @property
    def is_enabled(self):
        return self.job_config.get('enabled', False) and \
               self.table.job_config.get(self.import_job_type, {}).get('enabled', True) and \
               self.database.job_config.get(self.import_job_type, {}).get('enabled', True)  # noqa: E126

    @property
    def job_config(self):
        return self._job_config

    @property
    def import_job_type(self):
        return self._job_type
