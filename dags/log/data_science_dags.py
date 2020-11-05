"""
Data Science DAG Generator

Creates Dags based on configuration files `dags/data_science/configs/*.yaml`. For each DAG two
airflow variables can be defined to further configure the DAG:
{model_name}-internal-config and {model_name}-operational-config. For details please check
https://data-science.usehurrier.com/docs/internal/ds_tooling/.
"""

from datetime import timedelta
from typing import Dict, List, Any

from airflow import DAG
from airflow.models import Variable, Pool
from airflow.operators.dummy_operator import DummyOperator

from data_science import dag_configs, normalize_string, as_arg
from datahub.common import alerts
from operators.databricks_cluster_operator import DatabricksStartClusterOperator, DatabricksTerminateClusterOperator
from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator
from operators.utils.resources import Resources

# TODO when migrated fully to ldsutils v2 remove bucket variables
MODEL_STORE_BUCKET = Variable.get("data-science-model-store-bucket", default_var="staging-eu-data-science-model-store")
MODEL_OUTPUT_BUCKET = Variable.get("data-science-model-outputs-bucket", default_var="staging-eu-data-science-model-outputs")
DATA_LAKE_BUCKET = Variable.get("data-science-data-lake-bucket", default_var="staging-eu-data-science-data-lake")
S3_PROFILE = Variable.get("data-science-s3-profile", default_var="logistics-staging")
GOOGLE_PROFILE = Variable.get("data-science-google-profile", default_var="logistics-staging")
# TODO: this should be migrated to our own project
GOOGLE_PROJECT_ID = Variable.get("data-science-google-project-id", default_var="fulfillment-dwh-staging")
GOOGLE_CONN_ID = Variable.get("data-science-google-conn-id", default_var="bigquery_default")


def parse_operational_config(model_name):
    """
    Parses the operational configuration for a DS Dag from the airflow variables

    Examples
    --------
    >>> parse_operational_config(model_name="prep-times")
    {
        "countries": {
            "eu": ["ae", "at"],
            "us": ["py", "ar"]
        }
    }
    """

    return Variable.get(f"{model_name}-operational-config", default_var={}, deserialize_json=True)


def parse_internal_config(model_name):
    """
    Parses the internal configuration for a DS Dag from the airflow variables

    Examples
    --------
    >>> parse_internal_config(model_name="prep-times")
    {
        "image_version": "latest",
        "image_pull_policy": "Always",
        "google_conn_id": "bigquery_data_science",
        "env_vars": {
            "SOME_ENV_VAR": "3"
        },
        "retries": 1,
        "retry_delay": 5,
        "training_node_profile": "simulator-node",
        "resources": {
            "common": {
                "large": {
                    "countries": ["tw", "pk"],
                    "request_cpu": "400m",
                    "limit_cpu": "800m",
                    "request_memory": "400Mi",
                    "limit_memory": "800Mi"
                }
            },
            "task-specific": {
                "export-country-data": {
                    "default": {
                        "request_cpu": "1000m",
                        "limit_cpu": "1000m",
                        "request_memory": "4000",
                        "limit_memory": "24000Mi"
                    }
                },
                "process-country-data": {
                    "default": {
                        'request_cpu': '1000m',
                        'limit_cpu': '1000m',
                        'request_memory': '8000Mi',
                        'limit_memory': '42000Mi'
                    },
                "validate-country-data": {
                    "default": {
                        'request_cpu': '1000m',
                        'limit_cpu': '1000m',
                        'request_memory': '8000Mi',
                        'limit_memory': '42000Mi'
                    }
                },
                "train-country-model": {
                    "default": {
                        'request_cpu': '3900m',
                        'limit_cpu': '5000m',
                        'request_memory': '8000Mi',
                        'limit_memory': '42000Mi'
                     }
                },
                "validate-country-model": {
                    "default": {
                        'request_cpu': '1000m',
                        'limit_cpu': '2000m',
                        'request_memory': '8000Mi',
                        'limit_memory': '42000Mi'
                    }
                },
                "merge-region-models": {
                    "default": {
                        "request_cpu": "1000m",
                        "limit_cpu": "1000m",
                        "request_memory": "10000Mi",
                        "limit_memory": "10000Mi"
                    }
                },
                "validate-region-models": {
                    "default": {
                        'request_cpu': '1000m',
                        'limit_cpu': '2000m',
                        'request_memory': '10000Mi',
                        'limit_memory': '10000Mi',
                    }
                }
            }
        }
    }
    """

    return Variable.get(f"{model_name}-internal-config", default_var={}, deserialize_json=True)


def get_default_args(start_date, internal_config):
    return {
        'owner': 'data-science',
        'depends_on_past': False,
        'start_date': start_date,
        'retries': internal_config.get("retries", 1),
        'retry_delay': timedelta(minutes=internal_config.get("retry_delay", 5)),
        'executor_config': {
            'KubernetesExecutor': {
                'request_cpu': "200m",
                'limit_cpu': "200m",
                'request_memory': "4000Mi",
                'limit_memory': "4000Mi",
            }
        }
    }


def generate_dag(dag_config: Dict[str, Any],
                 internal_config: Dict[str, Any],
                 resources: Resources,
                 region: str,
                 units: List[str]):
    image = f"{dag_config['image']}:{internal_config.get('image_version', 'latest')}"
    image_pull_policy = internal_config.get('image_pull_policy', 'Always')
    model_name = dag_config['model_name']
    google_conn_id = internal_config.get('google_conn_id', GOOGLE_CONN_ID)

    env_vars = {
        "MODEL_STORE_BUCKET": MODEL_STORE_BUCKET,
        "MODEL_OUTPUT_BUCKET": MODEL_OUTPUT_BUCKET,
        "DATA_LAKE_BUCKET": DATA_LAKE_BUCKET,
        "S3_PROFILE": S3_PROFILE,
        "GOOGLE_PROFILE": GOOGLE_PROFILE,
        "GOOGLE_PROJECT_ID": GOOGLE_PROJECT_ID,
    }
    env_vars.update(internal_config.get("env_vars", {}))
    pools = internal_config.get('pools', {})

    # country_code to --country-code
    unit_arg_name = as_arg(dag_config["units"].get("key"))
    default_args = get_default_args(dag_config["start_date"], internal_config)

    dag = DAG(
        dag_id=f'train-{dag_config["model_name"]}-v{dag_config["version"]}-{region}',
        description=f'Processes input data for model: {dag_config["model_name"]}',
        default_args=default_args,
        catchup=False,
        tags=[default_args['owner']],
        schedule_interval=dag_config["schedules"][region],
    )

    start_dag_operator = DummyOperator(
        task_id=f'{model_name}-start-{region}',
        dag=dag,
    )

    end_dag_operator = DummyOperator(
        task_id=f'{model_name}-end-{region}',
        dag=dag,
    )

    query_data_steps = ['query-data', 'query-global-data']
    for query_data_step in query_data_steps:
        if query_data_step in dag_config["steps"]:
            query_data_operator = LogisticsKubernetesOperator(
                task_id=f'{model_name}-query-data-{region}',
                name=f'{model_name}-query-data-{region}',
                google_conn_id=google_conn_id,
                image=image,
                image_pull_policy=image_pull_policy,
                env_vars=env_vars,
                node_profile=internal_config.get("default_node_profile"),
                arguments=[
                    query_data_step,
                    '--execution-date', '{{next_ds}}'
                ],
                resources=resources.get(region, query_data_step),
                dag=dag,
                pool=pools.get(query_data_step, Pool.DEFAULT_POOL_NAME),
                step_name=query_data_step,
            )
            break

    start_dag_operator >> query_data_operator
    previous_global_operator = query_data_operator

    if dag_config.get('spark_cluster'):
        databricks_cluster_id = env_vars.get('DATABRICKS_CLUSTER_ID', None)

        start_spark_cluster_operator = DatabricksStartClusterOperator(
            task_id=f'start-spark-cluster-{region}',
            dag=dag,
            cluster_id=databricks_cluster_id,
        )

        end_spark_cluster_operator = DatabricksTerminateClusterOperator(
            task_id=f'terminate-spark-cluster-{region}',
            dag=dag,
            cluster_id=databricks_cluster_id,
        )

    # TODO: to stay downward compatible we allow to define different naming variations of steps
    merge_models_steps = ['merge-models', 'merge-region-models']
    for merge_models_step in merge_models_steps:
        if merge_models_step in dag_config["steps"]:
            merge_models_operator = LogisticsKubernetesOperator(
                task_id=f'{model_name}-merge-models-{region}',
                name=f'{model_name}-merge-models-{region}',
                google_conn_id=google_conn_id,
                image=image,
                image_pull_policy=image_pull_policy,
                env_vars=env_vars,
                node_profile=internal_config.get("default_node_profile"),
                arguments=[
                    merge_models_step,
                    '--region', region,
                    '--execution-date', '{{next_ds}}'
                ]
                + [arg for s in units for arg in (unit_arg_name, s)],
                resources=resources.get(region, merge_models_step),
                dag=dag,
                pool=pools.get(merge_models_step, Pool.DEFAULT_POOL_NAME),
                step_name='merge-models',
            )

            break

    for unit in units:
        # this.. is the glue that hols the world together!
        prev_unit_operator = previous_global_operator

        date_split_profile_args = [
            "--execution-date", "{{next_ds}}",
            unit_arg_name, unit,
        ]

        # TODO: again, these for loops are kept for downward compatiblity for different naming variations of steps
        export_data_steps = ['export-data', 'export-country-data']
        for export_data_step in export_data_steps:
            if export_data_step in dag_config["steps"]:
                export_data_operator = LogisticsKubernetesOperator(
                    task_id=f'{model_name}-export-data-{normalize_string(unit)}',
                    name=f'{model_name}-export-data-{normalize_string(unit)}',
                    google_conn_id=google_conn_id,
                    image=image,
                    image_pull_policy=image_pull_policy,
                    env_vars=env_vars,
                    node_profile=internal_config.get("default_node_profile"),
                    arguments=[export_data_step] + date_split_profile_args,
                    resources=resources.get(unit, export_data_step),
                    dag=dag,
                    pool=pools.get(export_data_step, Pool.DEFAULT_POOL_NAME),
                    step_name=export_data_step,
                )

                if dag_config.get('spark_cluster', {}).get('start_before') == export_data_step:
                    prev_unit_operator >> start_spark_cluster_operator >> export_data_operator
                else:
                    prev_unit_operator >> export_data_operator

                if dag_config.get('spark_cluster', {}).get('end_after') == export_data_step:
                    export_data_operator >> end_spark_cluster_operator

                prev_unit_operator = export_data_operator
                break

        process_data_steps = ['process-data', 'process-country-data']
        for process_data_step in process_data_steps:
            if process_data_step in dag_config["steps"]:
                process_data_operator = LogisticsKubernetesOperator(
                    task_id=f'{model_name}-process-data-{normalize_string(unit)}',
                    name=f'{model_name}-process-data-{normalize_string(unit)}',
                    google_conn_id=google_conn_id,
                    image=image,
                    image_pull_policy=image_pull_policy,
                    env_vars=env_vars,
                    node_profile=internal_config.get("default_node_profile"),
                    arguments=[process_data_step] + date_split_profile_args,
                    resources=resources.get(unit, process_data_step),
                    dag=dag,
                    pool=pools.get(process_data_step, Pool.DEFAULT_POOL_NAME),
                    step_name=process_data_step,
                )

                if dag_config.get('spark_cluster', {}).get('start_before') == process_data_step:
                    prev_unit_operator >> start_spark_cluster_operator >> process_data_operator
                else:
                    prev_unit_operator >> process_data_operator

                if dag_config.get('spark_cluster', {}).get('end_after') == process_data_step:
                    process_data_operator >> end_spark_cluster_operator

                prev_unit_operator = process_data_operator
                break

        validate_data_steps = ['validate-data', 'validate-country-data']
        for validate_data_step in validate_data_steps:
            if validate_data_step in dag_config["steps"]:
                validate_data_operator = LogisticsKubernetesOperator(
                    task_id=f'{model_name}-validate-data-{normalize_string(unit)}',
                    name=f'{model_name}-validate-data-{normalize_string(unit)}',
                    google_conn_id=google_conn_id,
                    image=image,
                    image_pull_policy=image_pull_policy,
                    node_profile=internal_config.get("default_node_profile"),
                    env_vars=env_vars,
                    arguments=[validate_data_step] + date_split_profile_args,
                    resources=resources.get(unit, validate_data_step),
                    dag=dag,
                    pool=pools.get(validate_data_step, Pool.DEFAULT_POOL_NAME),
                    step_name=validate_data_step,
                )

                if dag_config.get('spark_cluster', {}).get('start_before') == validate_data_step:
                    prev_unit_operator >> start_spark_cluster_operator >> validate_data_operator
                else:
                    prev_unit_operator >> validate_data_operator

                if dag_config.get('spark_cluster', {}).get('end_after') == validate_data_step:
                    validate_data_operator >> end_spark_cluster_operator

                prev_unit_operator = validate_data_operator

                break

        train_model_steps = ['train-model', 'train-country-model']
        for train_model_step in train_model_steps:
            if train_model_step in dag_config["steps"]:
                train_model_operator = LogisticsKubernetesOperator(
                    task_id=f'{model_name}-train-model-{normalize_string(unit)}',
                    name=f'{model_name}-train-model-{normalize_string(unit)}',
                    google_conn_id=google_conn_id,
                    image=image,
                    image_pull_policy=image_pull_policy,
                    node_profile=internal_config.get("training_node_profile",
                                                     internal_config.get("default_node_profile")),
                    pool=pools.get(train_model_step, Pool.DEFAULT_POOL_NAME),
                    env_vars=env_vars,
                    arguments=[train_model_step] + date_split_profile_args,
                    resources=resources.get(unit, train_model_step),
                    dag=dag,
                    on_success_callback=alerts.setup_callback(),
                    on_failure_callback=alerts.setup_callback(),
                    step_name=train_model_step,
                )

                if dag_config.get('spark_cluster', {}).get('start_before') == train_model_step:
                    prev_unit_operator >> start_spark_cluster_operator >> train_model_operator
                else:
                    prev_unit_operator >> train_model_operator

                if dag_config.get('spark_cluster', {}).get('end_after') == train_model_step:
                    train_model_operator >> end_spark_cluster_operator

                prev_unit_operator = train_model_operator
                break

        if "train-predict-model" in dag_config["steps"]:  # Batch prediction
            train_predict_model_operator = LogisticsKubernetesOperator(
                task_id=f'{model_name}-train-predict-model-{normalize_string(unit)}',
                name=f'{model_name}-train-predict-model-{normalize_string(unit)}',
                google_conn_id=google_conn_id,
                image=image,
                image_pull_policy=image_pull_policy,
                node_profile=internal_config.get("training_node_profile",
                                                 internal_config.get("default_node_profile")),
                pool=pools.get('train-predict-model', Pool.DEFAULT_POOL_NAME),
                env_vars=env_vars,
                arguments=['train-predict-model'] + date_split_profile_args,
                resources=resources.get(unit, "train-predict-model"),
                dag=dag,
                on_success_callback=alerts.setup_callback(),
                on_failure_callback=alerts.setup_callback(),
                step_name='train-predict-model',
            )

            if dag_config.get('spark_cluster', {}).get('start_before') == 'train-predict-model':
                prev_unit_operator >> start_spark_cluster_operator >> train_predict_model_operator
            else:
                prev_unit_operator >> train_predict_model_operator

            if dag_config.get('spark_cluster', {}).get('end_after') == 'train-predict-model':
                train_predict_model_operator >> end_spark_cluster_operator

            prev_unit_operator = train_predict_model_operator

        elif "run-optimization" in dag_config["steps"]:  # OPTIMIZATION STEP (analogous to train_predict_model)
            run_optimization_operator = LogisticsKubernetesOperator(
                task_id=f'{model_name}-run-optimization-{normalize_string(unit)}',
                name=f'{model_name}-run-optimization-{normalize_string(unit)}',
                google_conn_id=google_conn_id,
                image=image,
                image_pull_policy=image_pull_policy,
                node_profile=internal_config.get("training_node_profile",
                                                 internal_config.get("default_node_profile")),
                pool=pools.get('run-optimization', Pool.DEFAULT_POOL_NAME),
                env_vars=env_vars,
                arguments=['run-optimization'] + date_split_profile_args,
                resources=resources.get(unit, "run-optimization"),
                dag=dag,
                on_success_callback=alerts.setup_callback(),
                on_failure_callback=alerts.setup_callback(),
                step_name='run-optimization',
            )

            if dag_config.get('spark_cluster', {}).get('start_before') == 'run-optimization':
                prev_unit_operator >> start_spark_cluster_operator >> run_optimization_operator
            else:
                prev_unit_operator >> run_optimization_operator

            if dag_config.get('spark_cluster', {}).get('end_after') == 'run-optimization':
                run_optimization_operator >> end_spark_cluster_operator

            prev_unit_operator = run_optimization_operator

        validate_model_steps = ['validate-model', 'validate-country-model']
        for validate_model_step in validate_model_steps:
            if validate_model_step in dag_config["steps"]:
                validate_model_operator = LogisticsKubernetesOperator(
                    task_id=f'{model_name}-validate-model-{normalize_string(unit)}',
                    name=f'{model_name}-validate-model-{normalize_string(unit)}',
                    google_conn_id=google_conn_id,
                    image=image,
                    image_pull_policy=image_pull_policy,
                    node_profile=internal_config.get("default_node_profile"),
                    env_vars=env_vars,
                    arguments=[validate_model_step] + date_split_profile_args,
                    resources=resources.get(unit, validate_model_step),
                    dag=dag,
                    pool=pools.get(validate_model_step, Pool.DEFAULT_POOL_NAME),
                    step_name=validate_model_step,
                )

                if dag_config.get('spark_cluster', {}).get('start_before') == validate_model_step:
                    prev_unit_operator >> start_spark_cluster_operator >> validate_model_operator
                else:
                    prev_unit_operator >> validate_model_operator

                if dag_config.get('spark_cluster', {}).get('end_after') == validate_model_step:
                    validate_model_operator >> end_spark_cluster_operator

                prev_unit_operator = validate_model_operator

                break

        # Batch prediction
        if "publish-results" in dag_config["steps"]:
            publish_results_operator = LogisticsKubernetesOperator(
                task_id=f'{model_name}-publish-results-{normalize_string(unit)}',
                name=f'{model_name}-publish-results-{normalize_string(unit)}',
                google_conn_id=google_conn_id,
                image=image,
                image_pull_policy=image_pull_policy,
                node_profile=internal_config.get("default_node_profile"),
                pool=pools.get('publish-results', Pool.DEFAULT_POOL_NAME),
                env_vars=env_vars,
                arguments=['publish-results'] + date_split_profile_args,
                resources=resources.get(unit, "publish-results"),
                dag=dag,
                on_success_callback=alerts.setup_callback(),
                on_failure_callback=alerts.setup_callback(),
                step_name='publish-results',
            )

            if dag_config.get('spark_cluster', {}).get('start_before') == 'publish-results':
                prev_unit_operator >> start_spark_cluster_operator >> publish_results_operator
            else:
                prev_unit_operator >> publish_results_operator

            if dag_config.get('spark_cluster', {}).get('end_after') == 'publish-results':
                publish_results_operator >> end_spark_cluster_operator

            prev_unit_operator = publish_results_operator

        # if merge model step exists make it downstream task
        if [step for step in merge_models_steps if step in dag_config["steps"]]:
            for merge_models_step in merge_models_steps:
                if merge_models_step in dag_config["steps"]:
                    prev_unit_operator >> merge_models_operator
                    break
        else:
            prev_unit_operator >> end_dag_operator

    validate_models_steps = ['validate-models', 'validate-region-models']
    for validate_models_step in validate_models_steps:
        if validate_models_step in dag_config["steps"]:
            validate_region_models_operator = LogisticsKubernetesOperator(
                task_id=f"{model_name}-validate-models-{region}",
                name=f"{model_name}-validate-models-{region}",
                google_conn_id=google_conn_id,
                image=image,
                image_pull_policy=image_pull_policy,
                env_vars=env_vars,
                node_profile=internal_config.get("default_node_profile"),
                arguments=[
                    validate_models_step,
                    "--region", region,
                    "--execution-date", "{{next_ds}}",
                ]
                + [arg for s in units for arg in (unit_arg_name, s)],
                resources=resources.get(region, validate_models_step),
                dag=dag,
                pool=pools.get(validate_models_step, Pool.DEFAULT_POOL_NAME),
                step_name=validate_models_step,
            )

            merge_models_operator >> validate_region_models_operator
            validate_region_models_operator >> end_dag_operator

            break

    return dag


def generate_dags():
    for dag_config in dag_configs:
        operational_config = parse_operational_config(dag_config["model_name"])
        internal_config = parse_internal_config(dag_config["model_name"])

        resources = Resources(internal_config.get("resources"))

        for region, units in operational_config.get(dag_config.get("units").get("kind"), {}).items():
            dag = generate_dag(dag_config, internal_config, resources, region, units)
            globals()[dag.dag_id] = dag


generate_dags()
