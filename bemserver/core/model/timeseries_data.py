"""Timeseries data"""
import sqlalchemy as sqla

from bemserver.core.database import Base


class TimeseriesData(Base):
    __tablename__ = "timeseries_data"
    __table_args__ = (
        sqla.PrimaryKeyConstraint("timeseries_id", "timestamp"),
    )

    timestamp = sqla.Column(
        sqla.DateTime(timezone=True)
    )
    timeseries_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey('timeseries.id'),
        nullable=False,
    )
    timeseries = sqla.orm.relationship('Timeseries')
    value = sqla.Column(sqla.Float)
