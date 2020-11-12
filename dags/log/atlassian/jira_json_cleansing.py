import json
from datetime import datetime

import dateutil.parser
from airflow import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from pytz import utc


def recursive_key_op(key, var, op):
    if hasattr(var, 'items'):
        for k, v in list(var.items()):
            if k == key:
                if op == 'delete':
                    del var[k]
                if op == 'time_cast':
                    try:
                        if var[k]:
                            var[k] = dateutil.parser.parse(var[k]).astimezone(utc)
                        else:
                            var[k] = None
                    except Exception as e:
                        raise AirflowException(f'Error parsing {k}: {str(e)}')
            if isinstance(v, dict):
                recursive_key_op(key, v, op)
            elif isinstance(v, list):
                for d in list(v):
                    recursive_key_op(key, d, op)


class JiraJsonCleansing(LoggingMixin):

    def __init__(self, execution_date):
        super().__init__()
        self.ingested_at = datetime.now()
        self.execution_date = execution_date

    def prepare(self, issue):
        time_fields = ['updated', 'created', 'started', 'lastViewed', 'resolutiondate']
        blacklisted_fields = ['iconUrl', 'avatarUrls', 'self', 'avatarId', "startAt", "maxResults"]

        issue.raw['permalink'] = issue.permalink()
        if issue.raw.get('fields', None) is not None:
            fields = issue.raw.pop('fields')
            for blacklisted_field in blacklisted_fields:
                recursive_key_op(blacklisted_field, fields, 'delete')
            for time_field in time_fields:
                recursive_key_op(time_field, fields, 'time_cast')
            issue.raw = dict(issue.raw, **fields)

        created_at = issue.raw.pop("created")
        updated_at = issue.raw.pop("updated")
        default_rows = {
            'project_id': issue.raw.get("project").get("key"),
            '_ingested_at': self.ingested_at,
            'created_date': created_at.strftime('%Y-%m-%d'),
            'created_at': created_at,
            'updated_at': updated_at,
            'merge_layer_run_from': self.execution_date
        }

        return json.dumps(dict(issue.raw, **default_rows), default=self._json_encoder)

    @staticmethod
    def _delete_blacklisted_fields(blacklisted_fields, value):
        if isinstance(value, dict):
            for blacklisted_field in blacklisted_fields:
                if blacklisted_field in value:
                    del value[blacklisted_field]

    @staticmethod
    def _json_encoder(o):
        if isinstance(o, datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S.%f')
