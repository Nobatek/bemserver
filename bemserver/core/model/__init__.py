"""Model"""
from bemserver.core.database import db

from .timeseries import Timeseries  # noqa
from .timeseries_data import TimeseriesData  # noqa


db.hypertables.extend([
    ("timeseries_data", "timestamp"),
])
