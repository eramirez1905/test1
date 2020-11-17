from airflow.configuration import conf

from .development import CONFIG as DEVELOPMENT_CONFIG
from .production import CONFIG as PRODUCTION_CONFIG
from .staging import CONFIG as STAGING_CONFIG

ENVIRONMENT = conf.get("datahub", "environment")
IS_PRODUCTION = ENVIRONMENT == "production"
IS_STAGING = ENVIRONMENT == "staging"

CONFIG = DEVELOPMENT_CONFIG

if IS_STAGING:
    CONFIG = STAGING_CONFIG

if IS_PRODUCTION:
    CONFIG = PRODUCTION_CONFIG
