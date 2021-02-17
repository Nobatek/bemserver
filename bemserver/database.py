"""Databases: SQLAlchemy database and raw connection"""
import contextlib

from flask_sqlalchemy import SQLAlchemy
import psycopg2


db = SQLAlchemy()


class RawConnection:
    def __init__(self):
        self._conn_params = None

    def init_app(self, app):
        self._conn_params = {
            "host": app.config["DB_HOST"],
            "port": app.config["DB_PORT"],
            "user": app.config["DB_USER"],
            "password": app.config["DB_PWD"],
            "database": app.config["DB_DATABASE"],
        }

    @contextlib.contextmanager
    def connection(self):
        conn = psycopg2.connect(**self._conn_params)
        yield conn
        conn.commit()


rc = RawConnection()
