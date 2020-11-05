import requests

from airflow.hooks.http_hook import HttpHook


class HttpApiHook(HttpHook):
    def get_pandas_df(self, sql):
        raise NotImplementedError()

    def get_records(self, sql):
        raise NotImplementedError()

    def get_conn(self, headers=None):
        """
        Returns http session for use with requests

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """
        session = requests.Session()
        if self.http_conn_id:
            conn = self.get_connection(self.http_conn_id)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = schema + "://" + host

            if conn.port:
                self.base_url = self.base_url + ":" + str(conn.port)
            if conn.password:
                session.token = session.headers.update({
                    "Authorization": f"Bearer {conn.password}"
                })
            if conn.extra:
                try:
                    session.headers.update(conn.extra_dejson)
                except TypeError:
                    self.log.warning('Connection to %s has invalid extra field.', conn.host)
        if headers:
            session.headers.update(headers)

        return session
