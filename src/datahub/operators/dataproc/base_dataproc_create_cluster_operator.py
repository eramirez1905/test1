import os
import random
from abc import ABCMeta

from airflow import AirflowException
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator

from datahub.common.helpers import TableSettings
from googleapiclient.errors import HttpError


class BaseDataprocCreateClusterOperator(DataprocClusterCreateOperator, metaclass=ABCMeta):
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

        spark_properties = {
            'spark-env:JDBC_USER': self.read_replica_username,
            'spark-env:JDBC_PASSWORD': self.read_replica_password,
            'spark-env:IS_DATAPROC': "true"
        }

        super().__init__(project_id=config['dataproc']['project_id'],
                         cluster_name=cluster_name,
                         region=config['dataproc'].get('region', 'europe-west1'),
                         zone=self.get_zone,
                         image_version='1.4',
                         master_machine_type=self.spark_cluster.get("worker_node"),
                         worker_machine_type=self.spark_cluster.get("driver_node"),
                         num_workers=self.spark_cluster.get("num_workers"),
                         storage_bucket=config['dataproc'].get('storage_bucket', 'default'),
                         master_disk_type='pd-standard',
                         master_disk_size=20,
                         worker_disk_type='pd-standard',
                         worker_disk_size=20,
                         properties=spark_properties,
                         subnetwork_uri=config['dataproc'].get('subnetwork', 'default'),
                         internal_ip_only=True,
                         **kwargs
                         )

    def execute(self, context):
        if not self.is_enabled:
            self.log.info(f'Job "{self.import_job_type}" is disabled in config')
            return

        try:
            super().execute(context)
        except HttpError as e:
            # Check if cluster already exists, if it does, make task successful
            if e.resp.status == 409:
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

    @property
    def get_zone(self):
        return random.choice(self.config['dataproc'].get('zone', ['europe-west1-b']))

    @property
    def spark_cluster(self):
        return {**self.config.get('dataproc').get('default', {}).get('spark_cluster', {}),
                **self.config.get('dataproc').get(self.import_job_type, {}).get('default', {}).get('spark_cluster', {}),
                **self.table.spark_cluster.get(self.import_job_type, self.table.spark_cluster.get('default', {}))}

    @property
    def read_replica_username(self):
        return self.database.credentials.get('username', '')

    @property
    def read_replica_password(self):
        return os.getenv(self.database.credentials.get('password', ''), '')
