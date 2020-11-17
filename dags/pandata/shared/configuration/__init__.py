import os

from datahub.common.configuration import Config
from datahub.common.helpers import create_pool

common_config = Config()
common_config.add_config("config", f"{os.path.dirname(__file__)}/yaml")
common_config.add_config(
    "curated_data", f"{os.path.dirname(__file__)}/yaml/curated_data"
)
common_config.add_config("acl", f"{os.path.dirname(__file__)}/yaml/acl")

shared_config = common_config.config

for pool_name, pool_attributes in shared_config["pools"].items():
    create_pool(
        pool_attributes["name"],
        pool_attributes["slots"],
        pool_attributes["description"],
    )
