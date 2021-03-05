"""Timeseries data API schemas"""
import marshmallow as ma
from flask_smorest.fields import Upload

from bemserver.api import Schema, AutoSchema
from bemserver.model import TimeseriesData


class TimeseriesDataSchema(AutoSchema):
    class Meta:
        table = TimeseriesData.__table__


class TimeseriesDataQueryArgsSchema(Schema):
    """Timeseries values GET query parameters schema"""

    start_time = ma.fields.AwareDateTime(
        required=True,
        description='Initial datetime',
    )
    end_time = ma.fields.AwareDateTime(
        required=True,
        description='End datetime (excluded from the interval)',
    )
    timeseries = ma.fields.List(
        ma.fields.Int(),
        required=True,
        description='List of timeseries ID',
    )


class TimeseriesCSVFileSchema(ma.Schema):
    csv_file = Upload()
