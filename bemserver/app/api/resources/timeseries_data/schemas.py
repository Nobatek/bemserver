"""Timeseries data API schemas"""
import marshmallow as ma
from flask_smorest.fields import Upload

from bemserver.core.model import TimeseriesData

from bemserver.app.api import Schema, AutoSchema
from bemserver.app.api.extensions.ma_fields import Timezone


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


class TimeseriesDataAggregateQueryArgsSchema(TimeseriesDataQueryArgsSchema):
    """Timeseries values aggregate GET query parameters schema"""

    # TODO: Create custom field for bucket width
    bucket_width = ma.fields.String(
        required=True,
        description="Bucket width (ISO 8601 duration or PostgreSQL)",
    )
    timezone = Timezone(
        missing="UTC",
        description='Timezone to use for the aggreagation',
    )


class TimeseriesCSVFileSchema(ma.Schema):
    csv_file = Upload()
