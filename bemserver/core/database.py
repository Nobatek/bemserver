"""Databases: SQLAlchemy database access"""
import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base


SESSION_FACTORY = sessionmaker(autocommit=False, autoflush=False)
DB_SESSION = scoped_session(SESSION_FACTORY)
Base = declarative_base()


class DBConnection:
    """Database accessor"""

    def __init__(self):
        self.engine = None

    def set_db_url(self, db_url):
        """Set DB URL"""
        self.engine = sqla.create_engine(db_url, future=True)
        SESSION_FACTORY.configure(bind=self.engine)

    @property
    def session(self):
        return DB_SESSION

    def create_all(self):
        """Create all tables"""
        Base.metadata.create_all(bind=self.engine)

    def drop_all(self):
        """Drop all tables"""
        # Close all sessions before droping else drop_all can be stucked,
        #  especially when session is still in an open transaction.
        sqla.orm.session.close_all_sessions()
        Base.metadata.drop_all(bind=self.engine)

    def setup_tables(self):
        """Recreate database tables"""
        self.drop_all()
        self.create_all()

db = DBConnection()
