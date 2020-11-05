from datetime import timedelta

from airflow import AirflowException

from datahub.common import alerts
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator


def _get_ui_color(ui_color: str):
    if ui_color == 'report':
        return '#de870b'
    elif ui_color.startswith('#'):
        return ui_color
    else:
        raise AirflowException(f'UI color={ui_color} is invalid.')


class BigQueryTaskGenerator:
    def __init__(self, config, dag, search_path, prefix='', curated_data_tasks=None, full_import=False, pool_name_queries='curated_data_queries'):
        self.config = config
        self.tasks = {}
        self.dag = dag
        self.search_path = search_path
        self.prefix = prefix
        self.curated_data_tasks = curated_data_tasks or []
        self.full_import = full_import
        self.pool_name_queries = pool_name_queries

    def create_operator(self, table_name, params):
        file_path = params['file_path']
        operator = BigQueryOperator(
            dag=self.dag,
            task_id=f'create-table-{self.prefix}{table_name}',
            sql=f'{self.search_path}/{file_path}.sql',
            priority='INTERACTIVE',
            use_legacy_sql=False,
            params={
                'full_import': self.full_import
            },
            on_failure_callback=alerts.setup_callback(),
            execution_timeout=timedelta(minutes=25),
            pool=self.pool_name_queries,
        )

        return operator

    def add_operator(self, table: str, curated_data_dependencies=None, operator_dependencies=None, ui_color='#a28e0e'):
        if operator_dependencies is None:
            operator_dependencies = []
        if curated_data_dependencies is None:
            curated_data_dependencies = []

        table_name = table.split('/')[-1]
        if table_name not in self.tasks:
            self.tasks[table_name] = {
                'curated_data_dependencies': curated_data_dependencies,
                'operator_dependencies': operator_dependencies,
                'file_path': table,
                'ui_color': ui_color,
            }

    def add_downstream_task(self, name, dependency):
        self.tasks[name].set_downstram(dependency)

    def render(self):
        operators = {name: self.create_operator(name, params) for (name, params) in self.tasks.items()}

        for name, params in self.tasks.items():
            if params['ui_color'] is not None:
                try:
                    operators[name].ui_color = _get_ui_color(params['ui_color'])
                except KeyError as key:
                    raise AirflowException(f'Table {key} does not exist. table.name={name}')
            for dependency in params['curated_data_dependencies']:
                try:
                    operators[name].set_upstream(self.curated_data_tasks[dependency].tail)
                except KeyError as key:
                    raise AirflowException(f'Table {key} does not exist. table.name={name}, dependency={dependency}')
            for dependency in params['operator_dependencies']:
                try:
                    operators[name].set_upstream(operators[dependency])
                except KeyError as key:
                    raise AirflowException(f'Table {key} does not exist. table.name={name}, dependency={dependency}')
        return operators
