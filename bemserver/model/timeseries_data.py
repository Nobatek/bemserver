"""Timeseries data"""
import sqlalchemy as sqla

from bemserver.database import Base


class TimeseriesData(Base):
    __tablename__ = "timeseries_data"

    timestamp = sqla.Column(sqla.DateTime(timezone=True), primary_key=True)
    timeseries_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey('timeseries.id'),
        nullable=False,
        primary_key=True
    )
    timeseries = sqla.orm.relationship('Timeseries')
    value = sqla.Column(sqla.Float)
