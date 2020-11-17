from airflow.hooks.http_hook import HttpHook


class UseHurrierHook(HttpHook):
    """
    Interact with HTTP servers.
    :param http_conn_id: connection that has the base API url i.e https://www.google.com/
        and optional authentication credentials. Default headers can also be specified in
        the Extra field in json format.
    :type http_conn_id: str
    :param method: the API method to be called
    :type method: str
    """

    def __init__(
        self,
        country_code,
        method='POST',
        http_conn_id='usehurrier'
    ):
        super().__init__(method=method,
                         http_conn_id=http_conn_id)
        self.country_code = country_code

    # headers may be passed through directly or in the "extra" field in the connection
    # definition
    def get_conn(self, headers=None):
        """
        Returns http session for use with requests, allows interpolation of country_code in connection.
        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """
        session = super().get_conn(headers=headers)

        if self.http_conn_id:
            self.base_url = self.base_url.format(country_code=self.country_code)

        return session
