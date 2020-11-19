from os import path

import hiyapyco
from airflow import configuration as conf
from airflow.models import Variable

from datahub.common.helpers import create_pool


def load_config(config_name, base_path):
    base_config_path = path.realpath(base_path)
    yaml_files = [path.join(base_config_path, f"{config_name}.yaml"),
                  path.join(base_config_path, f"{config_name}_{conf.get('pandata', 'environment')}.yaml")]

    try:
        overwriting_config = Variable.get('configuration')
        yaml_files.append(overwriting_config)
    except KeyError:
        pass

    return hiyapyco.load(yaml_files, interpolate=True, failonmissingfiles=True, method=hiyapyco.METHOD_MERGE)


config = load_config("config", path.dirname(__file__))
iam_config = load_config("google_iam", path.dirname(__file__) + "/google_iam/yaml/")

for pool_name, pool_attributes in config['pools'].items():
    create_pool(pool_attributes['name'], pool_attributes['slots'], pool_attributes['description'])
