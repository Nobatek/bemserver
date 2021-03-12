"""Databases: SQLAlchemy database and raw connection"""
import contextlib

import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base


session_factory = sessionmaker(autocommit=False, autoflush=False)
db_session = scoped_session(session_factory)
Base = declarative_base()


class DBConnection:
    def __init__(self):
        self.engine = None

    def set_engine(self, db_url):
        self.engine = sqla.create_engine(db_url)
        session_factory.configure(bind=self.engine)

    def init_app(self, app):
        db_url = sqla.engine.url.URL(
            drivername="postgresql+psycopg2",
            username=app.config["DB_USER"],
            password=app.config["DB_PWD"],
            host=app.config["DB_HOST"],
            port=app.config["DB_PORT"],
            database=app.config["DB_DATABASE"],
        )
        self.set_engine(db_url)

        @app.teardown_appcontext
        def cleanup(_):
            db_session.remove()

    @property
    def session(self):
        return db_session

    def create_all(self):
        Base.metadata.create_all(bind=self.engine)

    def drop_all(self):
        Base.metadata.drop_all(bind=self.engine)

    @contextlib.contextmanager
    def raw_connection(self):
        try:
            conn = self.session.bind.raw_connection()
            yield conn
        finally:
            conn.close()


db = DBConnection()
