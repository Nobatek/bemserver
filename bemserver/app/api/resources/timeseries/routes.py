"""Timeseries resources"""

from flask.views import MethodView
from flask_smorest import abort

from bemserver.core.model import Timeseries

from bemserver.app.api import Blueprint, SQLCursorPage
from bemserver.app.database import db

from .schemas import TimeseriesSchema, TimeseriesQueryArgsSchema


blp = Blueprint(
    'Timeseries',
    __name__,
    url_prefix='/timeseries',
    description="Operations on timeseries"
)


@blp.route('/')
class TimeseriesViews(MethodView):

    @blp.etag
    @blp.arguments(TimeseriesQueryArgsSchema, location='query')
    @blp.response(200, TimeseriesSchema(many=True))
    @blp.paginate(SQLCursorPage)
    def get(self, args):
        """List timeseries"""
        return db.session.query(Timeseries).filter_by(**args)

    @blp.etag
    @blp.arguments(TimeseriesSchema)
    @blp.response(201, TimeseriesSchema)
    def post(self, new_item):
        """Add a new timeseries"""
        item = Timeseries(**new_item)
        db.session.add(item)
        db.session.commit()
        return item


@blp.route('/<int:item_id>')
class TimeseriesByIdViews(MethodView):

    @blp.etag
    @blp.response(200, TimeseriesSchema)
    def get(self, item_id):
        """Get timeseries by ID"""
        item = db.session.get(Timeseries, item_id)
        if item is None:
            abort(404)
        return item

    @blp.etag
    @blp.arguments(TimeseriesSchema)
    @blp.response(200, TimeseriesSchema)
    def put(self, new_item, item_id):
        """Update an existing timeseries"""
        item = db.session.get(Timeseries, item_id)
        if item is None:
            abort(404)
        blp.check_etag(item, TimeseriesSchema)
        TimeseriesSchema().update(item, new_item)
        db.session.add(item)
        db.session.commit()
        return item

    @blp.etag
    @blp.response(204)
    def delete(self, item_id):
        """Delete a timeseries"""
        item = db.session.get(Timeseries, item_id)
        if item is None:
            abort(404)
        blp.check_etag(item, TimeseriesSchema)
        db.session.delete(item)
        db.session.commit()
