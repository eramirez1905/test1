import os

import pendulum
import yaml
from airflow import configuration

DATA_SCIENCE_DAG_FOLDER = "{}/data_science/dag_configs".format(configuration.get('core', 'dags_folder'))


def parse_dag_configs():
    configs = []

    for config_file in os.listdir(DATA_SCIENCE_DAG_FOLDER):
        if not config_file.endswith("yaml"):
            continue

        with open(os.path.join(DATA_SCIENCE_DAG_FOLDER, config_file), "r") as f:
            raw_config = yaml.safe_load(f)
            raw_config["start_date"] = pendulum.parse(raw_config["start_date"])
            raw_config["version"] = int(raw_config["version"])

            raw_config["units"] = raw_config.get("units", {"kind": "countries", "key": "country_code"})

            configs.append(raw_config)

    return configs


def as_arg(x: str) -> str:
    return '--' + normalize_string(x)


def normalize_string(x: str) -> str:
    return x.lower().replace('_', '-')


dag_configs = parse_dag_configs()
