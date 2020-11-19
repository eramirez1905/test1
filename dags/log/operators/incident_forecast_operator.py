import json
from typing import Dict, Optional

from airflow.utils.decorators import apply_defaults

from configuration import config
from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator


class IncidentForecastOperator(LogisticsKubernetesOperator):

    template_fields = ('parameters', 'cmds', 'parameters_json', 'arguments',
                       'env_vars', 'labels', 'resources')

    @apply_defaults
    def __init__(self,
                 task_id: str,
                 cmds: str,
                 entrypoint: str,
                 parameters: Dict[str, str],
                 resources: Optional[Dict],
                 labels: Optional[Dict[str, str]] = None,
                 skip_sentry: bool = False,
                 *args,
                 **kwargs):

        self.cmds = cmds
        self.entrypoint = entrypoint
        self.version = parameters['version'] if 'version' in parameters else 'latest'
        self.labels = labels if labels is not None else {}
        self.skip_sentry = skip_sentry

        self.resources = resources

        self.parameters = parameters
        self.parameters_json = json.dumps(self.parameters)

        self.env_vars = config['order-forecast'].get('env_vars', {})
        if self.skip_sentry:
            self.env_vars.pop('SENTRY_ENDPOINT', None)

        super(IncidentForecastOperator, self).__init__(
            task_id=task_id,
            name=task_id.replace('_', '-'),
            image='683110685365.dkr.ecr.eu-west-1.amazonaws.com/order-forecast-runner:' + self.version,
            image_pull_policy='Always',
            cmds=[self.cmds],
            arguments=[self.entrypoint, self.parameters_json],
            node_profile='simulator-node',
            labels={
                'app': 'incident_forecast_airflow_operator',
                'country': self.parameters['country_iso'] if 'country_iso' in self.parameters else self.parameters['country_code'],
                'team': 'data-science',
                'version': str(self.version),
                **self.labels,
            },
            resources=self.resources,
            env_vars=self.env_vars,
            *args,
            **kwargs)
