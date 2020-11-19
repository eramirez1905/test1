import collections
import functools
import os

import hiyapyco
from airflow import AirflowException
from datahub.common.singleton import Singleton


class EntitiesConfig(metaclass=Singleton):
    __platform_types = ['hurrier_platforms', 'rps_platforms']

    def __init__(self, yaml_file=None) -> None:
        super().__init__()
        self.yaml_file = yaml_file if yaml_file is not None else f'{os.path.dirname(os.path.realpath(__file__))}/yaml/entities.yaml'

    @classmethod
    def clear_instance(cls):
        if cls in cls._instances:
            del cls._instances[cls]

    @property
    @functools.lru_cache(maxsize=None)
    def entities(self):
        entities = self.__get_entities_with_defaults(self.yaml_file)
        self.__validate_entities(entities)
        return entities

    def __get_entities_with_defaults(self, entities_yaml_file):
        entities_from_config_file = hiyapyco.load(entities_yaml_file,
                                                  interpolate=True,
                                                  failonmissingfiles=True,
                                                  method=hiyapyco.METHOD_MERGE)
        entities = entities_from_config_file.get('entities')
        for platforms in self.__platform_types:
            for entity in entities:
                if platforms not in entity:
                    entity[platforms] = []
                if entity.get('entity_id') not in entity.get(platforms):
                    entity.get(platforms).extend([entity.get('entity_id')])

        return entities

    def __validate_entities(self, entities_to_validate: dict):
        """
        Check if there are different entities with the same platform in the same country

        :param entities_to_validate:
        :return:
        """
        platforms = []
        for platform in self.__platform_types:
            if platform == 'hurrier_platforms':
                entities_to_validate = [e for e in entities_to_validate if len(e.get('country_code')) > 0]

                for entity in entities_to_validate:
                    for country in entity.get('country_code'):
                        platforms.extend([(country, e) for e in entity.get(platform, [])])
            else:
                for entity in entities_to_validate:
                    platforms.extend([(entity.get('country_iso'), e) for e in entity.get(platform, [])])

            duplicates = [item for item, count in collections.Counter(platforms).items() if count > 1]
            if len(duplicates) > 0:
                raise AirflowException(f'Found duplicated {platform}: {duplicates}')
