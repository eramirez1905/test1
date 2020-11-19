from datetime import datetime
import json


class SotiJsonCleansing:

    def __init__(self,
                 region,
                 execution_date):
        self.region = region
        self.ingested_at = datetime.now()
        self.execution_date = execution_date

    def prepare(self, obj):
        default_rows = {
            'region': self.region,
            '_ingested_at': self.ingested_at.strftime('%Y-%m-%d %H:%M:%S.%f'),
            'created_at': self.ingested_at.strftime('%Y-%m-%d %H:%M:%S.%f'),
            'updated_at': self.ingested_at.strftime('%Y-%m-%d %H:%M:%S.%f'),
            'created_date': self.ingested_at.strftime('%Y-%m-%d'),
            'merge_layer_run_from': self.execution_date
        }
        str_list = []

        for device in obj:
            if 'IMEI_MEID_ESN' in device and device['IMEI_MEID_ESN'] == 'Unavailable':
                device['IMEI_MEID_ESN'] = None
            if 'IMEI_MEID_ESN' in device and not isinstance(device['IMEI_MEID_ESN'], int):
                device['IMEI_MEID_ESN'] = None
            str_list.append(
                json.dumps(dict(device, **default_rows)).replace('$type', 'type')
            )

        return '\n'.join(str_list)
