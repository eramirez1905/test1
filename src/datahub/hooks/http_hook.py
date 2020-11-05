from abc import ABC

from airflow.hooks.http_hook import HttpHook as HttpHookBase


class HttpHookWithoutAuthentication(HttpHookBase, ABC):
    """
    This Hooks overwrites the HttpHook get connection method to remove the session authentication (user, password).
    This is needed because when getting the data from SOTI, we use an access token.
    If we were to set the user/password, the request would fail.
    """

    def get_conn(self, headers=None):
        session = super().get_conn()
        session.auth = None
        return session
