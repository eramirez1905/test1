import os

from configuration.transformations.clarisights import transform_clarisights_config
from configuration.transformations.ncr import (
    transform_ncr_config_to_dwh_merge_layer_databases,
)
from datahub.common.configuration import Config, ConfigTransformation
from datahub.common.helpers import create_pool

common_config = Config()

common_config.add_config("config", os.path.dirname(__file__))
common_config.add_config("acl", f"{os.path.dirname(__file__)}/yaml/acl")
common_config.add_config("policy_tags", f"{os.path.dirname(__file__)}/yaml/policy_tags")
common_config.add_config("audience", f"{os.path.dirname(__file__)}/yaml/audience")
common_config.add_config("clarisights", f"{os.path.dirname(__file__)}/yaml/clarisights")
common_config.add_transformation(
    ConfigTransformation(
        "clarisights", "dwh_merge_layer_databases", transform_clarisights_config
    )
)
common_config.add_config("ncr", f"{os.path.dirname(__file__)}/yaml/ncr")
common_config.add_transformation(
    ConfigTransformation(
        "ncr",
        "dwh_merge_layer_databases",
        transform_ncr_config_to_dwh_merge_layer_databases,
    )
)
common_config.add_config("vendor_user_mapping", f"{os.path.dirname(__file__)}/yaml/vendor_user_mapping")
config = common_config.config

for pool_name, pool_attributes in config["pools"].items():
    create_pool(
        pool_attributes["name"],
        pool_attributes["slots"],
        pool_attributes["description"],
    )
