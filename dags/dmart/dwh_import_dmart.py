from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

import dwh_import
from configuration import config
from datahub.common import alerts
from datahub.common.helpers import DataSource
from datahub.dwh_import_read_replica import DwhImportReadReplica

doc_md = f"""
<style type="text/css">
    div.dag-doc-md-legend-container {{
        display: block;
        padding-top: 2px;
        padding-bottom: 2px;
    }}
    span.dag-doc-md-legend-box {{
        display: table-cell;
        width: 20px;
        height: 20px;
        border: solid;
        border-width: 1px;
        border-color: darkblue;
    }}
    span.dag-doc-md-legend-label {{
        line-height: 12px;
        display: table-cell;
        vertical-align: middle;
        padding-left: 3px;
        padding-right: 3px;
        font-size: 0.75em;
    }}
</style>

### Legend

<div style="padding: 2px;color:#171212">
    <div class="dag-doc-md-legend-container">
        <span class="dag-doc-md-legend-box" style="background-color: #ffa729"></span>
        <span class="dag-doc-md-legend-label" title="Dataproc">Regional apps</span>
    </div>
    <div class="dag-doc-md-legend-container">
        <span class="dag-doc-md-legend-box" style="background-color: #ecd857"></span>
        <span class="dag-doc-md-legend-label" title="Dataproc">Country apps</span>
    </div>
    <div class="dag-doc-md-legend-container">
        <span class="dag-doc-md-legend-box" style="background-color: #9370DB"></span>
        <span class="dag-doc-md-legend-label" title="Dataproc">Dataproc</span>
    </div>
    <div class="dag-doc-md-legend-container">
        <span class="dag-doc-md-legend-box" style="background-color: #1a73e8"></span>
        <span class="dag-doc-md-legend-label" title="BigQuery queries">BigQuery queries</span>
    </div>
    <div class="dag-doc-md-legend-container">
        <span class="dag-doc-md-legend-box" style="background-color: #78a0f0"></span>
        <span class="dag-doc-md-legend-label" title="BigQuery queries">Create views</span>
    </div>
    <div class="dag-doc-md-legend-container">
        <span class="dag-doc-md-legend-box" style="background-color: #a3d6f1"></span>
        <span class="dag-doc-md-legend-label">Patch tables</span>
    </div>
    <div class="dag-doc-md-legend-container">
        <span class="dag-doc-md-legend-box" style="background-color: #fff7e6"></span>
        <span class="dag-doc-md-legend-label">Sanity checks</span>
    </div>
</div>


"""

version = 2
dag_prefix = 'import-dwh'


default_args = {
    'start_date': datetime(2020, 8, 20, 6, 0, 0),
    'end_date': None,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dwh_import_read_replicas = DwhImportReadReplica(config)

for database in dwh_import_read_replicas.databases.values():

    app_name = 'common'
    if database.source == DataSource.CLOUD_SQL:
        app_name = f'gcp-sql-{database.name}'
    elif database.source == DataSource.RDS:
        app_name = f'rds-{database.name}'

    dag_id = f'{dag_prefix}-{app_name}-v{version}'
    dag_parameters = {
        'description': f'Import data into DWH for {app_name}',
        'schedule_interval': config.get('dags').get(dag_prefix, {}).get('schedule_interval', '0 3,7,11,15,19,23 * * *'),
        'default_args': {**dwh_import.DEFAULT_ARGS, **default_args},
        'max_active_runs': 2,
        'concurrency': 10,
        'sla_miss_callback': alerts.alert_sla_miss,
        **config.get('dags').get(dag_id, {})
    }

    if dag_id in globals():
        dag = globals()[dag_id]
    else:
        dag = DAG(dag_id=dag_id, **dag_parameters)
        dag.doc_md = doc_md

    start = DummyOperator(
        dag=dag,
        task_id=f'start-{database.name}'
    )
    start.ui_color = '#afaf37' if database.is_regional else '#ffa729'
    start.ui_fgcolor = '#fff'
    for table in database.tables:
        export_cloud_sql = dwh_import_read_replicas.import_from_read_replicas(dag, table)
        start >> export_cloud_sql

    globals()[dag.dag_id] = dag
