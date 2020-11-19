from shared.configuration import shared_config

# This file is necessary only because the datahub framework requries it.
# We will not use this directory because it will be confused with the
# `config` directory so we are using the `shared` directory instead
config = shared_config
