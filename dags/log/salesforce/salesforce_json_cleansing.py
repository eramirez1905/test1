import json
from datetime import datetime


class SalesforceJsonCleansing:

    def __init__(self,
                 execution_date):
        self.ingested_at = datetime.now()
        self.execution_date = execution_date

    def prepare(self, obj):
        default_rows = {
            '_ingested_at': self.ingested_at.strftime('%Y-%m-%d %H:%M:%S.%f'),
            'created_at': self.ingested_at.strftime('%Y-%m-%d %H:%M:%S.%f'),
            'updated_at': self.ingested_at.strftime('%Y-%m-%d %H:%M:%S.%f'),
            'merge_layer_run_from': self.execution_date
        }
        str_list = []

        for record in obj['records']:
            if record.get('CreatedDate', None) is not None:
                record['created_date'] = datetime.strptime(record['CreatedDate'], "%Y-%m-%dT%H:%M:%S.%f%z").strftime('%Y-%m-%d')
                record['CreatedDate'] = datetime.strptime(record['CreatedDate'], "%Y-%m-%dT%H:%M:%S.%f%z").strftime('%Y-%m-%d %H:%M:%S.%f')
            else:
                record['created_date'] = self.ingested_at.strftime('%Y-%m-%d')
            if record.get('ClosedDate', None) is not None:
                record['ClosedDate'] = datetime.strptime(record['ClosedDate'], "%Y-%m-%dT%H:%M:%S.%f%z").strftime('%Y-%m-%d %H:%M:%S.%f')
            if record.get('LastModifiedDate', None) is not None:
                record['LastModifiedDate'] = datetime.strptime(record['LastModifiedDate'], "%Y-%m-%dT%H:%M:%S.%f%z").strftime('%Y-%m-%d %H:%M:%S.%f')
            if record.get('Due_Date__c', None) is not None:
                try:
                    record['Due_Date__c'] = datetime.strptime(record['Due_Date__c'], "%Y-%m-%d").strftime('%Y-%m-%d')
                except ValueError:
                    record['Due_Date__c'] = None
            str_list.append(
                json.dumps(dict(record, **default_rows))
            )

        return '\n'.join(str_list)
