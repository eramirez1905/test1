import os

from datahub.common.configuration import Config
from datahub.common.helpers import create_pool

common_config = Config()
common_config.add_config('config', f"{os.path.dirname(__file__)}/yaml")
common_config.add_config('curated_data', f"{os.path.dirname(__file__)}/yaml/curated_data")
common_config.add_config('dwh_imports', f"{os.path.dirname(__file__)}/yaml/dwh_imports")
common_config.add_config('acl', f"{os.path.dirname(__file__)}/yaml/acl")

config = common_config.config

for pool_name, pool_attributes in config['pools'].items():
    create_pool(pool_attributes['name'], pool_attributes['slots'], pool_attributes['description'])
