import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

import mkt_import
from configuration import config
from datahub.common import alerts
from datahub.dwh_import_read_replica import DwhImportReadReplica

dwh_import_read_replicas = DwhImportReadReplica(config)

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
for database in dwh_import_read_replicas.databases.values():
    args = {}
    project = database.extra_params.get("project_name", "")
    if project != "dwh_imports":
        continue

    app_version = database.extra_params.get("app_version", "1")
    start_date = database.extra_params.get("start_date")
    if start_date:
        args["start_date"] = pendulum.parse(start_date)

    dag_id = f"import-{database.name}-v{app_version}"
    dag_parameters = {
        "description": f"Import {database.name} data",
        "schedule_interval": "0 1 * * *",
        "default_args": {**mkt_import.DEFAULT_ARGS, **args},
        "sla_miss_callback": alerts.alert_sla_miss,
        "catchup": False,
        **config.get("dags").get(dag_id, {}),
    }

    if dag_id in globals():
        dag = globals()[dag_id]
    else:
        dag = DAG(dag_id=dag_id, **dag_parameters)
        dag.doc_md = doc_md

    start = DummyOperator(dag=dag, task_id=f"start-{database.name}")
    start.ui_color = "#afaf37" if database.is_regional else "#ffa729"
    start.ui_fgcolor = "#fff"
    for table in database.tables:
        export_cloud_sql = dwh_import_read_replicas.import_from_read_replicas(
            dag, table
        )
        start >> export_cloud_sql

    globals()[dag.dag_id] = dag
