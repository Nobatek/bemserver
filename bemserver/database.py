"""Databases: SQLAlchemy database and raw connection"""
import contextlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
import psycopg2


session_factory = sessionmaker()
Session = scoped_session(session_factory)
Base = declarative_base()


class SQLAlchemyConnection:
    def __init__(self):
        self.engine = None

    def init_app(self, app):
        self.engine = create_engine(app.config["SQLALCHEMY_DATABASE_URI"])
        session_factory.configure(bind=self.engine)

        @app.teardown_appcontext
        def cleanup(_):
            Session.remove()

    @property
    def session(self):
        return Session

    @property
    def Model(self):
        return Base

    def create_all(self):
        Base.metadata.create_all(bind=self.engine)

    def drop_all(self):
        Base.metadata.drop_all(bind=self.engine)


db = SQLAlchemyConnection()


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
        yield psycopg2.connect(**self._conn_params)


rc = RawConnection()
