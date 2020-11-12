import json

from airflow import configuration


def get_schema_filename():
    return '{}/freshdesk/schema.json'.format(configuration.get('core', 'dags_folder'))


def get_schema():
    with open(get_schema_filename()) as f:
        return json.load(f)


schema = get_schema()
