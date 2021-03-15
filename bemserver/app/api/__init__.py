"""BEMServer API"""
from .extensions import Api, Blueprint, Schema, AutoSchema, SQLCursorPage  # noqa
from .resources import register_blueprints


def init_app(app):
    api = Api()
    api.init_app(app)
    register_blueprints(api)
