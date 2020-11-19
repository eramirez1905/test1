"""
Legacy Data Science DAG Generator

Creates Dags based on configuration files `data_science/configs/*.yaml`. For each DAG two
airflow variables can be defined to further configure the DAG:
{model_name}-internal-config and {model_name}-operational-config. For details please check
https://data-science.usehurrier.com/docs/internal/ds_tooling/.
"""

from datetime import timedelta

from airflow import DAG
from airflow.models import Variable, Pool
from airflow.operators.dummy_operator import DummyOperator

from data_science import dag_configs
from datahub.common import alerts
from operators.databricks_cluster_operator import DatabricksStartClusterOperator, DatabricksTerminateClusterOperator
from operators.logistics_kubernetes_operator import LogisticsKubernetesOperator
from operators.utils.resources import Resources

# TODO when migrated fully to ldsutils v2 remove bucket variables
MODEL_STORE_BUCKET = Variable.get("data-science-model-store-bucket", default_var='staging-eu-data-science-model-store')
MODEL_OUTPUT_BUCKET = Variable.get("data-science-model-outputs-bucket", default_var='staging-eu-data-science-model-outputs')
DATA_LAKE_BUCKET = Variable.get("data-science-data-lake-bucket", default_var='staging-eu-data-science-data-lake')

S3_PROFILE = Variable.get("data-science-s3-profile", default_var='logistics-staging')
GOOGLE_PROFILE = Variable.get("data-science-google-profile", default_var='logistics-staging')


def parse_operational_config(model_name):
    """
    Parses the operational config for a DS Dag, e.g.:
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
    Parses the internal configuration for a DS Dag, e.g.:

    {
        "image_version": "latest",
        "image_pull_policy": "Always",
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


def generate_dag(dag_config, internal_config, resources, region, countries):
    image = f"{dag_config['image']}:{internal_config.get('image_version', 'latest')}"
    image_pull_policy = internal_config.get('image_pull_policy', 'Always')

    env_vars = {
        "MODEL_STORE_BUCKET": MODEL_STORE_BUCKET,
        "MODEL_OUTPUT_BUCKET": MODEL_OUTPUT_BUCKET,
        "DATA_LAKE_BUCKET": DATA_LAKE_BUCKET,
        "S3_PROFILE": S3_PROFILE,
        "GOOGLE_PROFILE": GOOGLE_PROFILE
    }
    env_vars.update(internal_config.get("env_vars", {}))
    pools = internal_config.get('pools', {})

    dag = DAG(
        dag_id=f'process-{dag_config["model_name"]}-v{dag_config["version"]}-{region}',
        description=f'Processes input data for model: {dag_config["model_name"]}',
        default_args=get_default_args(dag_config["start_date"], internal_config),
        catchup=False,
        schedule_interval=dag_config["schedules"][region],
    )

    start_dag_operator = DummyOperator(
        task_id=f'start_{region}_dag_operator',
        dag=dag,
    )

    end_dag_operator = DummyOperator(
        task_id=f'end_{region}_dag_operator',
        dag=dag,
    )

    query_global_data_operator = LogisticsKubernetesOperator(
        task_id=f'query-global-data-{region}',
        name=f'query-global-data-{region}',
        image=image,
        image_pull_policy=image_pull_policy,
        env_vars=env_vars,
        node_profile=internal_config.get("default_node_profile"),
        arguments=[
            'query-global-data',
            '--execution-date', '{{next_ds}}'
        ],
        resources=resources.get(region, "query-global-data"),
        dag=dag,
        pool=pools.get('query-global-data', Pool.DEFAULT_POOL_NAME)
    )

    start_dag_operator >> query_global_data_operator
    previous = query_global_data_operator

    if env_vars.get('SPARK_ON') and env_vars.get('DATABRICKS_CLUSTER_ID'):

        start_cluster = DatabricksStartClusterOperator(
            task_id=f'start-spark-cluster-{region}',
            dag=dag,
            cluster_id=env_vars.get('DATABRICKS_CLUSTER_ID')
        )

        terminate_cluster_operator = DatabricksTerminateClusterOperator(
            task_id=f'terminate-spark-cluster-{region}',
            dag=dag,
            cluster_id=env_vars.get('DATABRICKS_CLUSTER_ID')
        )

        previous >> start_cluster
        previous = start_cluster

    if "merge-region-models" in dag_config["steps"]:
        merge_models_operator = LogisticsKubernetesOperator(
            task_id=f'merge-region-models-{region}',
            name=f'merge-region-models-{region}',
            image=image,
            image_pull_policy=image_pull_policy,
            env_vars=env_vars,
            node_profile=internal_config.get("default_node_profile"),
            arguments=[
                'merge-region-models',
                '--region', region,
                '--execution-date', '{{next_ds}}'
            ]
            + [arg for c in countries for arg in ("--country-code", c)],
            resources=resources.get(region, "merge-region-models"),
            dag=dag,
            pool=pools.get('merge-region-models', Pool.DEFAULT_POOL_NAME)
        )

    for country in countries:
        date_country_profile_args = [
            "--execution-date", "{{next_ds}}",
            "--country-code", country,
        ]

        prev_operator = previous

        if "export-country-data" in dag_config["steps"]:
            export_data_operator = LogisticsKubernetesOperator(
                task_id=f'export-country-data-{country}',
                name=f'export-country-data-{country}',
                image=image,
                image_pull_policy=image_pull_policy,
                env_vars=env_vars,
                node_profile=internal_config.get("default_node_profile"),
                arguments=['export-country-data'] + date_country_profile_args,
                resources=resources.get(country, "export-country-data"),
                dag=dag,
                pool=pools.get('export-country-data', Pool.DEFAULT_POOL_NAME)
            )

            prev_operator >> export_data_operator
            prev_operator = export_data_operator

        if "process-country-data" in dag_config["steps"]:
            process_data_operator = LogisticsKubernetesOperator(
                task_id=f'process-country-data-{country}',
                name=f'process-country-{country}',
                image=image,
                image_pull_policy=image_pull_policy,
                env_vars=env_vars,
                node_profile=internal_config.get("default_node_profile"),
                arguments=['process-country-data'] + date_country_profile_args,
                resources=resources.get(country, "process-country-data"),
                dag=dag,
                pool=pools.get('process-country-data', Pool.DEFAULT_POOL_NAME)
            )

            prev_operator >> process_data_operator
            prev_operator = process_data_operator

        if "train-country-model" in dag_config["steps"]:
            train_model_operator = LogisticsKubernetesOperator(
                task_id=f'train-country-model-{country}',
                name=f'train-country-model-{country}',
                image=image,
                image_pull_policy=image_pull_policy,
                node_profile=internal_config.get("training_node_profile",
                                                 internal_config.get("default_node_profile")),
                pool=pools.get('train-country-model', Pool.DEFAULT_POOL_NAME),
                env_vars=env_vars,
                arguments=['train-country-model'] + date_country_profile_args,
                resources=resources.get(country, "train-country-model"),
                dag=dag,
                on_success_callback=alerts.setup_callback(),
                on_failure_callback=alerts.setup_callback(),
            )

            prev_operator >> train_model_operator
            prev_operator = train_model_operator

        elif "train-predict-model" in dag_config["steps"]:  # Batch prediction
            train_predict_model_operator = LogisticsKubernetesOperator(
                task_id=f'train-predict-model-{country}',
                name=f'train-predict-model-{country}',
                image=image,
                image_pull_policy=image_pull_policy,
                node_profile=internal_config.get("training_node_profile",
                                                 internal_config.get("default_node_profile")),
                pool=pools.get('train-predict-model', Pool.DEFAULT_POOL_NAME),
                env_vars=env_vars,
                arguments=['train-predict-model'] + date_country_profile_args,
                resources=resources.get(country, "train-predict-model"),
                dag=dag,
                on_success_callback=alerts.setup_callback(),
                on_failure_callback=alerts.setup_callback(),
            )

            prev_operator >> train_predict_model_operator
            prev_operator = train_predict_model_operator

            if env_vars.get('SPARK_ON') and env_vars.get('DATABRICKS_CLUSTER_ID'):

                prev_operator >> terminate_cluster_operator
                prev_operator = terminate_cluster_operator

        elif "run-optimization" in dag_config["steps"]:  # OPTIMIZATION STEP (analogous to train_predict_model)
            run_optimization_operator = LogisticsKubernetesOperator(
                task_id=f'run-optimization-{country}',
                name=f'run-optimization-{country}',
                image=image,
                image_pull_policy=image_pull_policy,
                node_profile=internal_config.get("training_node_profile",
                                                 internal_config.get("default_node_profile")),
                pool=pools.get('run-optimization', Pool.DEFAULT_POOL_NAME),
                env_vars=env_vars,
                arguments=['run-optimization'] + date_country_profile_args,
                resources=resources.get(country, "run-optimization"),
                dag=dag,
                on_success_callback=alerts.setup_callback(),
                on_failure_callback=alerts.setup_callback(),
            )

            prev_operator >> run_optimization_operator
            prev_operator = run_optimization_operator

        if "validate-country-model" in dag_config["steps"]:
            validate_model_operator = LogisticsKubernetesOperator(
                task_id=f'validate-country-model-{country}',
                name=f'validate-country-model-{country}',
                image=image,
                image_pull_policy=image_pull_policy,
                node_profile=internal_config.get("default_node_profile"),
                env_vars=env_vars,
                arguments=['validate-country-model'] + date_country_profile_args,
                resources=resources.get(country, "validate-country-model"),
                dag=dag,
                pool=pools.get('validate-country-model', Pool.DEFAULT_POOL_NAME)
            )

            prev_operator >> validate_model_operator
            prev_operator = validate_model_operator

        # Batch prediction
        if "publish-results" in dag_config["steps"]:
            publish_results_operator = LogisticsKubernetesOperator(
                task_id=f'publish-results-{country}',
                name=f'publish-results-{country}',
                image=image,
                image_pull_policy=image_pull_policy,
                node_profile=internal_config.get("default_node_profile"),
                pool=pools.get('publish-results', Pool.DEFAULT_POOL_NAME),
                env_vars=env_vars,
                arguments=['publish-results'] + date_country_profile_args,
                resources=resources.get(country, "publish-results"),
                dag=dag,
                on_success_callback=alerts.setup_callback(),
                on_failure_callback=alerts.setup_callback(),
            )

            prev_operator >> publish_results_operator
            prev_operator = publish_results_operator

        if "merge-region-models" in dag_config["steps"]:
            prev_operator >> merge_models_operator
        else:
            prev_operator >> end_dag_operator

    if "validate-region-models" in dag_config["steps"]:
        validate_region_models_operator = LogisticsKubernetesOperator(
            task_id=f"validate-region-models-{region}",
            name=f"validate-region-models-{region}",
            image=image,
            image_pull_policy=image_pull_policy,
            env_vars=env_vars,
            node_profile=internal_config.get("default_node_profile"),
            arguments=[
                "validate-region-models",
                "--region", region,
                "--execution-date", "{{next_ds}}",
            ]
            + [arg for c in countries for arg in ("--country-code", c)],
            resources=resources.get(region, "validate-region-models"),
            dag=dag,
            pool=pools.get('validate-region-models', Pool.DEFAULT_POOL_NAME),
        )

        merge_models_operator >> validate_region_models_operator
        validate_region_models_operator >> end_dag_operator

    return dag


def generate_dags():
    for dag_config in dag_configs:
        operational_config = parse_operational_config(dag_config["model_name"])
        internal_config = parse_internal_config(dag_config["model_name"])
        resources = Resources(internal_config.get("resources"))

        for region in operational_config.get('countries', []):
            countries = operational_config["countries"][region]
            dag = generate_dag(dag_config, internal_config, resources, region, countries)
            globals()[dag.dag_id] = dag


generate_dags()
