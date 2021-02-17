"""Timeseries resources"""

from flask.views import MethodView
from sqlalchemy.sql.expression import func

from bemserver.api import Blueprint
from bemserver.database import db
from bemserver.model import TimeseriesData

from .schemas import TimeseriesDataSchema, TimeseriesQueryArgsSchema


blp = Blueprint(
    'TimeseriesData',
    __name__,
    url_prefix='/timeseries-data',
    description="Operations on timeseries data"
)


@blp.route('/<int:timeseries_id>')
class TimeseriesViews(MethodView):

    @blp.arguments(TimeseriesQueryArgsSchema, location='query')
    @blp.response(200, TimeseriesDataSchema(many=True))
    def get(self, args, timeseries_id):
        data = db.session.query(
            func.timezone("UTC", TimeseriesData.timestamp).label("timestamp"),
            TimeseriesData.value,
        ).filter(
            TimeseriesData.timeseries_id == timeseries_id
        ).filter(
            args["start_time"] <= TimeseriesData.timestamp
        ).filter(
            TimeseriesData.timestamp < args["end_time"]
        ).order_by(
            TimeseriesData.timestamp
        ).all()
        return data
