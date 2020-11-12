import os
import random
from abc import ABCMeta, abstractmethod

from airflow import AirflowException
from airflow import conf as configuration
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from datahub.common.helpers import TableSettings


class BaseDatabricksJobOperator(DatabricksSubmitRunOperator, metaclass=ABCMeta):
    ui_color = '#9370DB'

    def __init__(self, config, job_name, table: TableSettings = None, **kwargs):
        if job_name not in config['databricks']:
            raise AirflowException(f"Missing config for Databricks job {job_name}")

        self.database = table.database
        self.table = table
        self.config = config

        self._job_name = job_name
        self._job_config = config['databricks'][job_name]

        if not self.is_enabled:
            self.ui_color = '#000000'

        super().__init__(json=self._build_job_json(), **kwargs)

    @property
    def job_name(self):
        return self._job_name

    @property
    def job_config(self):
        return self._job_config

    @property
    def version(self):
        return self.job_config['version']

    @property
    def jar_path(self):
        return self.job_config['jar_path'].format(version=self.version)

    @property
    def main_class(self):
        return self.job_config['main_class']

    @property
    def is_enabled(self):
        return self.job_config.get('enabled', False) and \
               self.table.job_config.get(self.job_name, {}).get('enabled', True) and \
               self.database.job_config.get(self.job_name, {}).get('enabled', True)  # noqa: E126

    @property
    @abstractmethod
    def job_params(self):
        pass

    def prepare_template(self):
        self.json['spark_jar_task']['parameters'] = self.job_params
        self.log.info("Prepare job template %s:%s with parameters: %s",
                      self.job_name, self.version, self.json['spark_jar_task']['parameters'])

    def execute(self, context):
        if not self.is_enabled:
            self.log.info(f'Job "{self.job_name}" is disabled in config')
            return

        super().execute(context)

    def _build_job_json(self):
        return {
            "new_cluster": self._get_spark_cluster_spec(),
            "libraries": [
                {"jar": self.jar_path}
            ],
            "spark_jar_task": {
                "main_class_name": self.main_class
            },
            "email_notifications": {
                "on_start": self.job_config.get('notifications', []),
                "on_success": self.job_config.get('notifications', []),
                "on_failure": self.job_config.get('notifications', [])
            },
            "timeout_seconds": self.job_config.get('timeout_seconds', 0),
            "retry_on_timeout": False,
            "max_retries": 0,
            "max_concurrent_runs": 1000
        }

    def _get_spark_cluster_spec(self):
        environment = configuration.get('datahub', 'environment')
        return {
            "spark_version": "apache-spark-2.4.x-scala2.11",
            "spark_conf": {
                "spark.hadoop.fs.s3.impl": "com.databricks.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.canned.acl": "BucketOwnerFullControl",
                "spark.databricks.delta.preview.enabled": "true",
                "spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl",
                "spark.driver.extraJavaOptions": "-Dlog4j.configDebug=true",
                "spark.hadoop.fs.s3n.impl": "com.databricks.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.impl": "com.databricks.s3a.S3AFileSystem"
            },
            "aws_attributes": {
                "availability": "SPOT_WITH_FALLBACK",
                "zone_id": self.get_zone_id,
                "instance_profile_arn": "arn:aws:iam::487596255802:instance-profile/dwh-staging-eu-databricks_profile",
                "spot_bid_price_percent": 100,
                "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                "ebs_volume_count": 1,
                "ebs_volume_size": 100
            },
            "driver_node_type_id": self.spark_cluster.get("driver_node"),
            "node_type_id": self.spark_cluster.get("worker_node"),
            "num_workers": self.spark_cluster.get("num_workers"),
            "custom_tags": {
                "Application": "dwh-export",
                "Context": "fulfillment-dwh",
                "Environment": "production-eu",
                "Module": "spark",
                "Service": "dwh",
                "Team": "data-engineering",
                "Tribe": "data",
                "Unit": "logistics",
            },
            "cluster_log_conf": {
                "dbfs": {
                    "destination": "dbfs:/cluster-logs"
                }
            },
            "spark_env_vars": {
                # The following credentials are used to access to the DWH database and the read replicas
                "JDBC_USER": self.read_replica_username,
                "JDBC_PASSWORD": self.read_replica_password,
                "PGOPTIONS": "-c statement_timeout=0",
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
                "GOOGLE_APPLICATION_CREDENTIALS": "/databricks/google-cloud-key.json",
                "ENVIRONMENT": environment,
            },
            "init_scripts": [
                {
                    "dbfs": {
                        "destination": f"dbfs:/databricks/init_scripts/{environment}/init_json_key.sh"
                    }
                },
                {
                    "dbfs": {
                        "destination": "dbfs:/databricks/init_scripts/set_spark_embedded_metastore.sh"
                    }
                }
            ],
        }

    @property
    def get_zone_id(self):
        return random.choice(['eu-west-1a', 'eu-west-1b', 'eu-west-1c'])

    @property
    def read_replica_username(self):
        return self.database.credentials.get('username', '')

    @property
    def read_replica_password(self):
        return os.getenv(self.database.credentials.get('password', ''), '')

    @property
    def spark_cluster(self):
        return {**self.config.get('databricks').get('default', {}).get('spark_cluster', {}),
                **self.config.get('databricks').get(self._job_name, {}).get('default', {}).get('spark_cluster', {}),
                **self.table.spark_cluster.get(self._job_name, self.table.spark_cluster.get('default', {}))}
