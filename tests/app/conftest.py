"""App-specific conftest"""
import os

import pytest

from dotenv import load_dotenv

from bemserver.core.database import db
from bemserver.app import create_app
from bemserver.app.settings import Config


load_dotenv('.env')


class TestConfig(Config):
    SQLALCHEMY_DATABASE_URI = os.getenv("TEST_SQLALCHEMY_DATABASE_URI")
    TESTING = True


@pytest.fixture
def app():
    application = create_app(TestConfig)
    db.setup_tables()
    yield application
    db.session.remove()
