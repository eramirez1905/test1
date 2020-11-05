from abc import ABC
import tenacity

from airflow.hooks.http_hook import HttpHook as HttpHookBase


class HttpHookExtended(HttpHookBase, ABC):
    """
    This Hooks overwrites the HttpHook run_with_advanced_retry  method to return the responce.
    It is needed because we are running Airflow 1.10.2 and it was missing return statement.
    However it was fixed in 1.10.4, https://issues.apache.org/jira/browse/AIRFLOW-4174
    """

    def run_with_advanced_retry(self, _retry_args, *args, **kwargs):
        """
        Runs Hook.run() with a Tenacity decorator attached to it. This is useful for
        connectors which might be disturbed by intermittent issues and should not
        instantly fail.
        :param _retry_args: Arguments which define the retry behaviour.
            See Tenacity documentation at https://github.com/jd/tenacity
        :type _retry_args: dict
        .. code-block:: python
            hook = HttpHook(http_conn_id='my_conn',method='GET')
            retry_args = dict(
                 wait=tenacity.wait_exponential(),
                 stop=tenacity.stop_after_attempt(10),
                 retry=requests.exceptions.ConnectionError
             )
             hook.run_with_advanced_retry(
                     endpoint='v1/test',
                     _retry_args=retry_args
                 )
        """
        self._retry_obj = tenacity.Retrying(
            **_retry_args
        )

        return self._retry_obj(super().run, *args, **kwargs)
