import json
from abc import ABC

from airflow.models import BaseOperator

from datahub.common.helpers import DatabaseSettings
from datahub.hooks.http_api_hook import HttpApiHook


class KafkaConnectBaseOperator(BaseOperator, ABC):
    def __init__(self, database: DatabaseSettings, project_id, dataset_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.payload = None
        self.database = database
        self.project_id = project_id
        self.dataset_id = dataset_id
        self._hook = None

    def _update_connect_config(self, config_name, database_password=None):
        payload = json.loads(self.payload)
        self.log.info("Kafka connect payload: %s", payload)

        if database_password is not None:
            payload["database.password"] = database_password

        try:
            devices_response = self.hook.run(
                endpoint=f"/connectors/{config_name}/config",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                json=payload,
            )
        except Exception as e:
            self.log.exception("Kafka connect config update failed")
            raise e
        self.log.info("Kafka connect response %s", devices_response)

    @property
    def hook(self) -> HttpApiHook:
        if self._hook is None:
            self._hook = HttpApiHook(
                method='PUT',
                http_conn_id='debezium_default'
            )
        return self._hook
