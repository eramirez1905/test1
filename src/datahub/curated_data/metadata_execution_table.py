from configuration import config


class MetadataExecutionTable:
    def __init__(self):
        self.dataset_id = config.get('bigquery').get('dataset').get('cl')
        self.table_id = 'dag_executions'
