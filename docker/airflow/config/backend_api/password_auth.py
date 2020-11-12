from __future__ import unicode_literals

import base64
from functools import wraps
from sys import version_info

from airflow.utils.db import create_session
from airflow.utils.log.logging_mixin import LoggingMixin
from flask import Response
from flask import make_response
from flask_appbuilder.security.sqla import models
from werkzeug.security import check_password_hash

log = LoggingMixin().log
PY3 = version_info[0] == 3

CLIENT_AUTH = None


class AuthenticationError(Exception):
    pass


def authenticate(session, username, password):
    """
    Authenticate a PasswordUser with the specified
    username/password.

    :param session: An active SQLAlchemy session
    :param username: The username
    :param password: The password

    :raise AuthenticationError: if an error occurred
    :return: a PasswordUser
    """
    if not username or not password:
        raise AuthenticationError()

    user = session.query(models.User).filter(
        models.User.username == username).first()

    if not user:
        raise AuthenticationError()

    if not check_password_hash(user.password, password):
        raise AuthenticationError()

    log.info("User %s successfully authenticated", username)
    return user


def _unauthorized():
    """
    Indicate that authorization is required
    :return:
    """
    return Response("Unauthorized", 401, {"WWW-Authenticate": "Basic"})


def _forbidden():
    return Response("Forbidden", 403)


def init_app(app):
    pass


def requires_authentication(function):
    @wraps(function)
    def decorated(*args, **kwargs):
        from flask import request

        header = request.headers.get("Authorization")
        if header:
            userpass = ''.join(header.split()[1:])
            username, password = base64.b64decode(userpass).decode("utf-8").split(":", 1)

            with create_session() as session:
                try:
                    authenticate(session, username, password)

                    response = function(*args, **kwargs)
                    response = make_response(response)
                    return response

                except AuthenticationError:
                    return _forbidden()

        return _unauthorized()
    return decorated
