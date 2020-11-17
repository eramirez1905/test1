from datetime import datetime, timedelta

from airflow import DAG, configuration
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

import dwh_import
from configuration import config
from datahub.common.bigquery_task_generator import BigQueryTaskGenerator
from datahub.operators.scorecard.scorecard_score_assignment_operator import ScorecardScoreAssignmentOperator
from datahub.operators.scorecard.scorecard_scores_to_values_operator import ScorecardScoresToValuesOperator


version = 2

default_args = {
    'owner': 'data-bi',
    'start_date': datetime(2019, 2, 25),
    'retries': 2,
    'concurrency': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

dags_folder = configuration.get('core', 'dags_folder')
search_path = 'scorecard/sql'

pandas_score_assignment_task_id = 'pandas_score_assignment'
pandas_score_assignment_zones_task_id = 'pandas_score_assignment_zones'
dummy_pandas_score_assignment_task_id = 'dummy_pandas_score_assignment'
dummy_pandas_scores_to_values_zones_task_id = 'dummy_pandas_scores_to_values_zones'

dag = DAG(
    dag_id=f'scorecard-v{version}',
    description=f'Scorecard',
    schedule_interval='0 0,8 * * 1',
    default_args={**dwh_import.DEFAULT_ARGS, **default_args},
    max_active_runs=1,
    template_searchpath=f'{dags_folder}/{search_path}',
    catchup=False)

task_generator = BigQueryTaskGenerator(dag=dag, config=config, search_path=search_path)

task_generator.add_operator(table='scorecard_dispatching')
task_generator.add_operator(table='scorecard_dispatching_zones')
task_generator.add_operator(table='scorecard_infra_prep')
task_generator.add_operator(table='scorecard_infra', operator_dependencies=[
    "scorecard_infra_prep",
])
task_generator.add_operator(table='scorecard_infra_zones_shapes')
task_generator.add_operator(table='scorecard_infra_zones', operator_dependencies=[
    "scorecard_infra_zones_shapes",
])
task_generator.add_operator(table='scorecard_rider_compliance')
task_generator.add_operator(table='scorecard_rider_compliance_zones')
task_generator.add_operator(table='scorecard_staffing')
task_generator.add_operator(table='scorecard_staffing_zones')
task_generator.add_operator(table='scorecard_vendor_compliance')
task_generator.add_operator(table='scorecard_vendor_compliance_zones')
task_generator.add_operator(table='scorecard_final_dataset', operator_dependencies=[
    "scorecard_rider_compliance",
    "scorecard_vendor_compliance",
    "scorecard_dispatching",
    "scorecard_infra",
    "scorecard_staffing",
])
task_generator.add_operator(table='scorecard_final_dataset_zones', operator_dependencies=[
    "scorecard_rider_compliance_zones",
    "scorecard_vendor_compliance_zones",
    "scorecard_dispatching_zones",
    "scorecard_infra_zones",
    "scorecard_staffing_zones",
])
task_generator.add_operator(table='scorecard_report')
task_generator.add_operator(table='scorecard_report_zones')
task_generator.add_operator(table='scorecard_weights')


def is_the_first_day_of_a_quarter(execution_date, prev_execution_date, templates_dict, **kwargs):
    is_first_execution_of_the_month = execution_date.month != prev_execution_date.month
    if is_first_execution_of_the_month and execution_date.month in [3, 6, 9, 12]:
        branch_task_id = templates_dict['branch_true_task_id']
    else:
        branch_task_id = templates_dict['branch_false_task_id']

    print(f'execution_date={execution_date}, prev_execution_date={prev_execution_date}, branch_task_id={branch_task_id}')
    return branch_task_id


pandas_score_assignment_zones = ScorecardScoreAssignmentOperator(
    dag=dag,
    task_id=pandas_score_assignment_zones_task_id,
    target_table='rl.scorecard_kpi_scores_zones',
    sql='pandas/final_dataset_zones.sql',
    executor_config={
        'KubernetesExecutor': {
            'request_cpu': "400m",
            'limit_cpu': "400m",
            'request_memory': "2048Mi",
            'limit_memory': "2048Mi",
        }
    },
)

pandas_score_assignment = ScorecardScoreAssignmentOperator(
    dag=dag,
    task_id=pandas_score_assignment_task_id,
    target_table='rl.scorecard_kpi_scores',
    sql='pandas/final_dataset.sql',
    executor_config={
        'KubernetesExecutor': {
            'request_cpu': "400m",
            'limit_cpu': "400m",
            'request_memory': "2048Mi",
            'limit_memory': "2048Mi",
        }
    },
)

branch_pandas_scores_to_values_zones = BranchPythonOperator(
    dag=dag,
    task_id='branch_pandas_scores_to_values_zones',
    python_callable=is_the_first_day_of_a_quarter,
    provide_context=True,
    templates_dict={
        'branch_true_task_id': pandas_score_assignment_zones_task_id,
        'branch_false_task_id': dummy_pandas_scores_to_values_zones_task_id,
        'execution_date': '{{ execution_date }}',
        'prev_execution_date': '{{ prev_execution_date }}',
    }
)

branch_pandas_score_assignment = BranchPythonOperator(
    dag=dag,
    task_id='branch_pandas_score_assignment',
    python_callable=is_the_first_day_of_a_quarter,
    provide_context=True,
    templates_dict={
        'branch_true_task_id': pandas_score_assignment_task_id,
        'branch_false_task_id': dummy_pandas_score_assignment_task_id,
        'execution_date': '{{ execution_date }}',
        'prev_execution_date': '{{ prev_execution_date }}',
    }
)

pandas_scores_to_values_zones = ScorecardScoresToValuesOperator(
    dag=dag,
    task_id='pandas_scores_to_values_zones',
    target_table='rl.scorecard_final_zones_table',
    sql='pandas/final_dataset_zones_comprehensive.sql',
    sql_score='pandas/scorecard_kpi_scores_zones.sql',
    executor_config={
        'KubernetesExecutor': {
            'request_cpu': "400m",
            'limit_cpu': "400m",
            'request_memory': "2048Mi",
            'limit_memory': "2048Mi",
        }
    },
)

pandas_scores_to_values = ScorecardScoresToValuesOperator(
    dag=dag,
    task_id='pandas_scores_to_values',
    target_table='rl.scorecard_final_table',
    sql='pandas/final_dataset_comprehensive.sql',
    sql_score='pandas/scorecard_kpi_scores.sql',
    executor_config={
        'KubernetesExecutor': {
            'request_cpu': "400m",
            'limit_cpu': "400m",
            'request_memory': "2048Mi",
            'limit_memory': "2048Mi",
        }
    },
)

dummy_pandas_scores_to_values_zones = DummyOperator(
    dag=dag,
    task_id=dummy_pandas_scores_to_values_zones_task_id,
)

dummy_pandas_score_assignment = DummyOperator(
    dag=dag,
    task_id=dummy_pandas_score_assignment_task_id,
)

join_pandas_score_assignment = DummyOperator(
    dag=dag,
    task_id='join_pandas_score_assignment',
    trigger_rule=TriggerRule.NONE_FAILED,
)

join_pandas_scores_to_values_zones = DummyOperator(
    dag=dag,
    task_id='join_pandas_scores_to_values_zones',
    trigger_rule=TriggerRule.NONE_FAILED,
)

start = DummyOperator(
    dag=dag,
    task_id='start',
)

operators = task_generator.render()

start >> operators.values()
start >> [branch_pandas_scores_to_values_zones, branch_pandas_score_assignment]

operators.get('scorecard_final_dataset_zones') >> pandas_score_assignment_zones
operators.get('scorecard_weights') >> pandas_score_assignment_zones
branch_pandas_scores_to_values_zones >> [dummy_pandas_scores_to_values_zones, pandas_score_assignment_zones] >> join_pandas_scores_to_values_zones
join_pandas_scores_to_values_zones >> pandas_scores_to_values_zones

operators.get('scorecard_final_dataset') >> pandas_score_assignment
operators.get('scorecard_weights') >> pandas_score_assignment

branch_pandas_score_assignment >> [dummy_pandas_score_assignment, pandas_score_assignment] >> join_pandas_score_assignment
join_pandas_score_assignment >> pandas_scores_to_values

operators.get('scorecard_final_dataset_zones') >> pandas_scores_to_values_zones
operators.get('scorecard_final_dataset') >> pandas_scores_to_values

pandas_scores_to_values >> operators.get('scorecard_report')
pandas_scores_to_values_zones >> operators.get('scorecard_report_zones')
