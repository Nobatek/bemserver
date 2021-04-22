"""Databases: SQLAlchemy database access"""
import contextlib

import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base


SESSION_FACTORY = sessionmaker(autocommit=False, autoflush=False)
DB_SESSION = scoped_session(SESSION_FACTORY)
Base = declarative_base()


class DBConnection:
    """Database accessor"""

    def __init__(self):
        self.engine = None
        # List of (table, time column) tuples to create hypertables on
        self.hypertables = []

    def set_db_url(self, db_url):
        """Set DB URL"""
        self.engine = sqla.create_engine(db_url, future=True)
        SESSION_FACTORY.configure(bind=self.engine)

    @property
    def session(self):
        return DB_SESSION

    @contextlib.contextmanager
    def raw_connection(self):
        """Provide a direct connection to the database"""
        try:
            conn = self.session.bind.raw_connection()
            yield conn
        finally:
            conn.close()

    def create_all(self):
        """Create all tables"""
        Base.metadata.create_all(bind=self.engine)

    def drop_all(self):
        """Drop all tables"""
        Base.metadata.drop_all(bind=self.engine)

    def create_hypertables(self):
        """Create Timescale hypertables"""
        with self.raw_connection() as conn, conn.cursor() as cur:
            for table, column in self.hypertables:
                query = (
                    "SELECT create_hypertable("
                    f"'{table}', '{column}', "
                    "create_default_indexes => False"
                    ");"
                )
                cur.execute(query)
            conn.commit()

    def setup_tables(self):
        """Recreate database tables"""
        self.drop_all()
        self.create_all()
        self.create_hypertables()


db = DBConnection()
