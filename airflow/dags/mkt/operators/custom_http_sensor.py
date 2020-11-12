from airflow.exceptions import AirflowException
from airflow.sensors.http_sensor import HttpSensor


class CustomHttpSensor(HttpSensor):
    """
    This is a customized version of the standard Airflow HttpSensor as in
    https://github.com/apache/airflow/blob/1.10.10/airflow/sensors/http_sensor.py#L76-L92

    The main difference is to have `**context` available in `response_check` function call.

    This becomes obsolete in Airflow >1.10.11 see
    https://github.com/apache/airflow/blob/master/airflow/providers/http/sensors/http.py#L102-L104
    """

    def poke(self, context):
        self.log.info("Poking: %s", self.endpoint)
        try:
            response = self.hook.run(
                self.endpoint,
                data=self.request_params,
                headers=self.headers,
                extra_options=self.extra_options,
            )
            if self.response_check:
                # run content check on response
                return self.response_check(response, **context)
        except AirflowException as ae:
            if str(ae).startswith("404"):
                return False

            raise ae

        return True
