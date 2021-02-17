"""Timeseries API schemas"""
import marshmallow as ma
import marshmallow_sqlalchemy as msa

from bemserver.api import Schema, AutoSchema
from bemserver.model import Timeseries


class TimeseriesSchema(AutoSchema):
    class Meta:
        table = Timeseries.__table__

    id = msa.auto_field(dump_only=True)
    name = msa.auto_field(validate=ma.validate.Length(1, 80))
    description = msa.auto_field(validate=ma.validate.Length(1, 500))
    unit = msa.auto_field(validate=ma.validate.Length(1, 20))


class TimeseriesQueryArgsSchema(Schema):
    name = ma.fields.Str()
    unit = ma.fields.Str()
