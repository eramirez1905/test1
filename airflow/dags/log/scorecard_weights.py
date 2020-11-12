from datetime import datetime, timedelta

from airflow import DAG, configuration

import dwh_import
from configuration import config
from datahub.common import bigquery_task_generator
from datahub.operators.scorecard_weights.variable_importances_rider_compliance_operator import \
    VariableImportancesRiderComplianceOperator
from datahub.operators.scorecard_weights.variable_importances_vendor_compliance_operator import \
    VariableImportancesVendorComplianceOperator
from datahub.operators.scorecard_weights.variable_importances_dispatching_operator import VariableImportancesDispatchingOperator
from datahub.operators.scorecard_weights.variable_importances_infra_operator import VariableImportancesInfraOperator
from datahub.operators.scorecard_weights.variable_importances_staffing_operator import VariableImportancesStaffingOperator

version = 1

default_args = {
    'owner': 'data-bi',
    'start_date': datetime(2019, 2, 25),
    'retries': 2,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

dags_folder = configuration.get('core', 'dags_folder')
search_path = 'scorecard_weights/sql'

doc_md = f"""
#### Add a description here
"""
dag = DAG(
    dag_id=f'scorecard-weights-v{version}',
    description=f'Scorecard Weights',
    schedule_interval='0 8 1 * *',
    default_args={**dwh_import.DEFAULT_ARGS, **default_args},
    max_active_runs=1,
    template_searchpath=f'{dags_folder}/{search_path}',
    catchup=False)

dag.doc_md = doc_md

task_generator = bigquery_task_generator.BigQueryTaskGenerator(dag=dag, config=config, search_path=search_path)

task_generator.add_operator(table='scorecard_dispatching_weights')
task_generator.add_operator(table='scorecard_infra_weights_prep')
task_generator.add_operator(table='scorecard_infra_weights', operator_dependencies=[
    "scorecard_infra_weights_prep",
])
task_generator.add_operator(table='scorecard_rider_compliance_weights')
task_generator.add_operator(table='scorecard_staffing_weights')
task_generator.add_operator(table='scorecard_vendor_compliance_weights')

variable_importances_rider_compliance = VariableImportancesRiderComplianceOperator(
    dag=dag,
    task_id='variable_importances_rider_compliance',
    executor_config={
        'KubernetesExecutor': {
            'request_cpu': "400m",
            'limit_cpu': "400m",
            'request_memory': "5000Mi",
            'limit_memory': "5000Mi",
        }
    },
)

variable_importances_vendor_compliance = VariableImportancesVendorComplianceOperator(
    dag=dag,
    task_id='variable_importances_vendor_compliance',
    executor_config={
        'KubernetesExecutor': {
            'request_cpu': "400m",
            'limit_cpu': "400m",
            'request_memory': "5000Mi",
            'limit_memory': "5000Mi",
        }
    },
)

variable_importances_dispatching = VariableImportancesDispatchingOperator(
    dag=dag,
    task_id='variable_importances_dispatching',
    executor_config={
        'KubernetesExecutor': {
            'request_cpu': "400m",
            'limit_cpu': "400m",
            'request_memory': "5000Mi",
            'limit_memory': "5000Mi",
        }
    },
)

variable_importances_staffing = VariableImportancesStaffingOperator(
    dag=dag,
    task_id='variable_importances_staffing',
    executor_config={
        'KubernetesExecutor': {
            'request_cpu': "400m",
            'limit_cpu': "400m",
            'request_memory': "5000Mi",
            'limit_memory': "5000Mi",
        }
    },
)

variable_importances_infra = VariableImportancesInfraOperator(
    dag=dag,
    task_id='variable_importances_infra',
    executor_config={
        'KubernetesExecutor': {
            'request_cpu': "400m",
            'limit_cpu': "400m",
            'request_memory': "8000Mi",
            'limit_memory': "8000Mi",
        }
    },
)

operators = task_generator.render()

operators.get('scorecard_rider_compliance_weights') >> variable_importances_rider_compliance
operators.get('scorecard_vendor_compliance_weights') >> variable_importances_vendor_compliance
operators.get('scorecard_dispatching_weights') >> variable_importances_dispatching
operators.get('scorecard_staffing_weights') >> variable_importances_staffing
operators.get('scorecard_infra_weights') >> variable_importances_infra
