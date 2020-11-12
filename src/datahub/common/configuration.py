import os
from collections import OrderedDict
from contextlib import suppress
from dataclasses import dataclass
from typing import List

import hiyapyco
from airflow import conf
from airflow.models import Variable

from datahub.common.singleton import Singleton


@dataclass(frozen=True)
class ConfigTransformation:
    source: str
    target: str
    callback: callable


class GenericConfig(metaclass=Singleton):

    def __init__(self) -> None:
        super().__init__()
        self._config_variable = None
        self._config = None
        self._yaml_files = []
        self.transformations: List[ConfigTransformation] = []

    @property
    def config(self):
        if self._config is None:
            self._render()

        return self._config

    def add_config(self, filename, base_path):

        base_config_path = os.path.realpath(base_path)
        config_file = os.path.join(base_config_path, f"{filename}.yaml")
        env_specific_file = os.path.join(base_config_path, f"{filename}_{conf.get('datahub', 'environment')}.yaml")
        config_files = [config_file, env_specific_file]

        if config_files not in self._yaml_files:
            self._yaml_files.append(config_file)
            if os.path.isfile(env_specific_file):
                self._yaml_files.append(env_specific_file)
            self._config = None

    def add_transformation(self, transformation: ConfigTransformation):
        self.transformations.append(transformation)
        self._config = None

    def _render(self):
        overwriting_config = []
        if self._config_variable is not None:
            with suppress(KeyError):
                overwriting_config = Variable.get(self._config_variable)
        self._config = hiyapyco.load(self._yaml_files + [overwriting_config], interpolate=True, failonmissingfiles=True,
                                     method=hiyapyco.METHOD_MERGE)
        self._apply_transformations()
        self._validate()

    def _apply_transformations(self):
        for transformation in self.transformations:
            if transformation.target not in self._config:
                self._config[transformation.target] = OrderedDict()
            new_config = transformation.callback(transformation.source, self._config)
            self._config[transformation.target].update(new_config)

    def _validate(self):
        pass

    @classmethod
    def clear_instance(cls):
        if cls in cls._instances:
            del cls._instances[cls]


class Config(GenericConfig):
    def __init__(self):
        super().__init__()
        self.add_config('config', f'{format(os.getenv("AIRFLOW_HOME"))}/src/datahub/configuration/yaml')
        self.add_config('spark', f'{format(os.getenv("AIRFLOW_HOME"))}/src/datahub/configuration/yaml/spark')
        self._config_variable = 'configuration'

    def _validate(self):
        datasets = self._config.get('bigquery').get('dataset')
        missing_datasets = [key for key, dataset_id in datasets.items() if dataset_id is None]
        if len(missing_datasets) > 0:
            raise Exception("Datasets '{}' not defined.".format(",".join(missing_datasets)))


class IamConfig(GenericConfig):
    def __init__(self):
        super().__init__()
