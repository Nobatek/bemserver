"""Weather Tools Server"""
import flask
import click

from .database import db, rc
from . import model
from . import api


@click.command()
@flask.cli.with_appcontext
def setup_db():
    db.drop_all()
    db.create_all()
    model.create_hypertables()


def create_app():
    app = flask.Flask(__name__)
    app.config.from_object("bemserver.settings.Config")
    app.config.from_envvar('FLASK_SETTINGS_FILE', silent=True)

    db.init_app(app)
    rc.init_app(app)
    api.init_app(app)

    app.cli.add_command(setup_db)

    return app
