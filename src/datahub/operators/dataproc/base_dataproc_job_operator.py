from abc import ABCMeta, abstractmethod

from airflow import AirflowException
from airflow.contrib.operators.dataproc_operator import DataProcSparkOperator

from datahub.common.helpers import TableSettings


class BaseDataprocJobOperator(DataProcSparkOperator, metaclass=ABCMeta):
    ui_color = '#9370DB'

    def __init__(self, config, import_job_type, table: TableSettings = None, **kwargs):
        if import_job_type not in config['dataproc']:
            raise AirflowException(f"Missing config for Dataproc job {import_job_type}")

        self.database = table.database
        self.table = table
        self.config = config

        self._job_type = import_job_type
        self._job_config = config['dataproc'][import_job_type]

        if not self.is_enabled:
            self.ui_color = '#000000'
            self.ui_fgcolor = '#FFF'

        super().__init__(
            dataproc_spark_jars=[self.job_config['jar_path'].format(version=self.job_config['version'])],
            main_class=self.job_config['main_class'],
            arguments=self.job_params,
            region=config['dataproc'].get('region', 'europe-west1'),
            **kwargs)

    def execute(self, context):
        if not self.is_enabled:
            self.log.info(f'Job "{self.import_job_type}" is disabled in config')
            return

        super().execute(context)

    @property
    @abstractmethod
    def job_params(self):
        pass

    @property
    def import_job_type(self):
        return self._job_type

    @property
    def job_config(self):
        return self._job_config

    @property
    def is_enabled(self):
        return self.job_config.get('enabled', False) and \
               self.table.job_config.get(self.import_job_type, {}).get('enabled', True) and \
               self.database.job_config.get(self.import_job_type, {}).get('enabled', True)  # noqa: E126
