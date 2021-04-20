"""BEMServer API"""
from .extensions import Api, Blueprint, Schema, AutoSchema, SQLCursorPage  # noqa
from .extensions.ma_fields import Timezone
from .resources import register_blueprints


def init_app(app):
    api = Api()
    api.init_app(app)
    api.register_field(Timezone, 'string', 'IANA timezone')
    register_blueprints(api)
