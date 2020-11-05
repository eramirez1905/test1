import json
from datetime import datetime

from airflow import AirflowException


class GodroidJsonCleansing:

    def __init__(self,
                 region,
                 execution_date):
        self.region = region
        self.ingested_at = datetime.now()
        self.execution_date = execution_date

    def prepare(self, obj):

        def gen_dict_extract(key, var):
            for k, v in var.items():
                if isinstance(v, dict):
                    return gen_dict_extract(key, v)
                if k == key:
                    return var
            return None

        strftime_ingested_at = self.ingested_at.strftime('%Y-%m-%d %H:%M:%S.%f')
        default_rows = {
            'region': self.region,
            '_ingested_at': strftime_ingested_at,
            'created_at': strftime_ingested_at,
            'updated_at': strftime_ingested_at,
            'created_date': self.ingested_at.strftime('%Y-%m-%d'),
            'merge_layer_run_from': self.execution_date
        }
        str_list = []
        for device in obj.values():
            device = gen_dict_extract('id', device)
            if device is None:
                raise AirflowException(f'Failed to parse data from Firebase devices for {self.region}')
            vendor_rows = []
            if 'vendors' in device:
                for vendor_id, vendor_details in device['vendors'].items():
                    if 'deliverPlatform' in vendor_details:
                        vendor_details['deliveryPlatform'] = vendor_details.pop('deliverPlatform')
                    elif 'deliveryPlatfrom' in vendor_details:
                        vendor_details['deliveryPlatform'] = vendor_details.pop('deliveryPlatfrom')
                    if 'name' in vendor_details:
                        if isinstance(vendor_details["name"], int):
                            vendor_details["name"] = str(vendor_details["name"])
                    vendor_details["vendor_id"] = vendor_id
                    vendor_rows.append(vendor_details)
            if vendor_rows:
                device['vendors'] = vendor_rows
            str_list.append(
                json.dumps(dict(device, **default_rows))
            )
        return '\n'.join(str_list)
