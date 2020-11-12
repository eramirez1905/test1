import itertools
import json
from datetime import datetime

from airflow import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin


class CloudHealthJsonCleansing(LoggingMixin):
    """
    This class transform the data presented in OLAP CloudHealth Reports into a series of rows
    Documentation: https://apidocs.cloudhealthtech.com/#reporting_understand-report-data-format
    """
    def __init__(self, report_type, execution_date):
        super().__init__()
        self.ingested_at = datetime.now()
        self.report_type = report_type
        self.execution_date = execution_date

    def prepare(self, report: dict):
        report_name = report["report"]
        dimensions, time_range = self.flat_dimensions(report)
        flat_data = self.flat_data(report["data"])

        i = 0
        final = []
        for f in itertools.product(time_range, dimensions):
            dimension = f[1]
            created_date = f[0]["name"]
            row = {
                "created_date": created_date,
                "name": report_name,
                "type": self.report_type,
                "dimension_name": f"{dimension['name']}",
                "dimension_label": dimension["label"],
                "data": flat_data[i],
                "created_at": f"{created_date}T00:00:00Z",
                "measures": report["measures"],
                "updated_at": report["updated_at"],
                "_ingested_at": self.ingested_at,
                "merge_layer_run_from": self.execution_date
            }
            final.append(row)
            i = i + 1

        str_list = [json.dumps(row, default=self._json_encoder, allow_nan=False) for row in final if self.__is_valid_row(row)]
        if len(str_list) == 0:
            raise AirflowException("CloudHealth import failed. No records found.")

        return '\n'.join(str_list) + '\n'

    @staticmethod
    def flat_dimensions(report):
        time_range = []
        dimensions = []

        for dimension in report["dimensions"]:
            for key in dimension.keys():
                if key == "time":
                    time_range = dimension["time"]
                else:
                    dimensions = dimension[key]
        return dimensions, time_range

    @staticmethod
    def __is_valid_row(row):
        foo = row.get("dimension_label") != "Total" and row.get("created_date") != "total" and row.get("data") is not None
        return foo

    @staticmethod
    def flat_data(report_data):
        if "error" in report_data:
            raise AirflowException(f"Data error: {report_data['error']}")

        flatted = list(itertools.chain.from_iterable(report_data))
        return list(itertools.chain.from_iterable(flatted))

    @staticmethod
    def _json_encoder(o):
        if isinstance(o, datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S.%f')
