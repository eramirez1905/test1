import json
from datetime import datetime
from typing import Dict, Optional

from airflow.utils.decorators import apply_defaults

from configuration import config
from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator


class OrderForecastOperator(LogisticsKubernetesOperator):
    template_fields = ('s3_input_folder', 's3_output_folder', 's3_input_bucket', 's3_output_bucket',
                       'parameters', 'parameters_opt',
                       'cmds', 'parameters_json', 'arguments',
                       'env_vars', 'labels',
                       'resources')

    @apply_defaults
    def __init__(self,
                 task_id: str,
                 country_code: str,
                 s3_input_bucket: str,
                 s3_input_folder: str,
                 s3_output_bucket: str,
                 s3_output_folder: str,
                 cmds: str,
                 entrypoint: str,
                 parameters: Dict[str, str],
                 resources: Optional[Dict],
                 version: str = 'latest',
                 labels: Optional[Dict[str, str]] = None,
                 skip_sentry: bool = False,
                 *args,
                 **kwargs):
        self.country_code = country_code
        self.s3_input_bucket = s3_input_bucket
        self.s3_input_folder = s3_input_folder
        self.s3_output_bucket = s3_output_bucket
        self.s3_output_folder = s3_output_folder
        self.cmds = cmds
        self.entrypoint = entrypoint
        self.version = version
        self.parameters_opt = parameters if parameters is not None else {}
        self.labels = labels if labels is not None else {}
        self.skip_sentry = skip_sentry

        self.parameters = {
            'country_code': country_code,
            'request_id': f'{country_code}/{datetime.now().strftime("%Y%m%d%H%M%S")}',
            'input_bucket': s3_input_bucket,
            'input_folder': s3_input_folder,
            'output_bucket': s3_output_bucket,
            'output_folder': s3_output_folder,
            'version': version,
            **self.parameters_opt
        }

        self.env_vars = config['order-forecast'].get('env_vars', {})
        if self.skip_sentry:
            self.env_vars.pop('SENTRY_ENDPOINT', None)

        self.resources = resources
        self.parameters_json = json.dumps(self.parameters)

        super(OrderForecastOperator, self).__init__(
            task_id=task_id,
            name=task_id.replace('_', '-'),
            image='683110685365.dkr.ecr.eu-west-1.amazonaws.com/order-forecast-runner:' + self.version,
            image_pull_policy='Always',
            cmds=[self.cmds],
            arguments=[self.entrypoint, self.parameters_json],
            node_profile=config['order-forecast'].get('node_profile', 'simulator-node'),
            labels={
                'app': 'order_forecast_airflow_operator',
                'country': self.country_code,
                'team': 'data-science',
                'version': str(self.version),
                **self.labels,
            },
            resources=self.resources,
            env_vars=self.env_vars,
            *args,
            **kwargs)
