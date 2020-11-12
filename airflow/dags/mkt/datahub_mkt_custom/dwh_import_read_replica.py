"""
Use MktDwhImportReadReplica to spin up a Datahub import framework using a customized key instead
of the hardcoded `dwh_merge_layer_databases`.
"""

from datahub.dwh_import_read_replica import DwhImportReadReplica


class MktDwhImportReadReplica(DwhImportReadReplica):
    def __init__(
        self, config, databases_config_key="dwh_merge_layer_databases", *args, **kwargs
    ):
        super(MktDwhImportReadReplica, self).__init__(config, *args, **kwargs)

        self.databases_config = config[databases_config_key]
