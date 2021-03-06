"""Timeseries resources"""
import io

from flask import Response
from flask_smorest import abort

from bemserver.core.csv_io import tscsvio
from bemserver.core.exceptions import TimeseriesCSVIOError

from bemserver.app.api import Blueprint

from .schemas import (
    TimeseriesDataQueryArgsSchema,
    TimeseriesDataAggregateQueryArgsSchema,
    TimeseriesCSVFileSchema,
)


blp = Blueprint(
    'TimeseriesData',
    __name__,
    url_prefix='/timeseries-data',
    description="Operations on timeseries data"
)


@blp.route('/', methods=('GET', ))
@blp.arguments(TimeseriesDataQueryArgsSchema, location='query')
@blp.response(200)
def get_csv(args):
    """Get timeseries data as CSV file"""
    csv_str = tscsvio.export_csv(
        args['start_time'],
        args['end_time'],
        args['timeseries']
    )

    response = Response(csv_str, mimetype='text/csv')
    response.headers.set(
        "Content-Disposition",
        "attachment",
        filename="timeseries.csv"
    )
    return response


@blp.route('/aggregate', methods=('GET', ))
@blp.arguments(TimeseriesDataAggregateQueryArgsSchema, location='query')
@blp.response(200)
def get_aggregate_csv(args):
    """Get aggregated timeseries data as CSV file"""
    csv_str = tscsvio.export_csv_bucket(
        args['start_time'],
        args['end_time'],
        args['timeseries'],
        args['bucket_width'],
        args['timezone'],
        args['aggregation'],
    )

    response = Response(csv_str, mimetype='text/csv')
    response.headers.set(
        "Content-Disposition",
        "attachment",
        filename="timeseries.csv"
    )
    return response


# TODO: document response
# https://github.com/marshmallow-code/flask-smorest/issues/142
@blp.route('/', methods=('POST', ))
@blp.arguments(TimeseriesCSVFileSchema, location='files')
@blp.response(201)
def post_csv(files):
    """Post timeseries data as CSV file"""
    csv_file = files['csv_file']
    with io.TextIOWrapper(csv_file) as csv_file_txt:
        try:
            tscsvio.import_csv(csv_file_txt)
        except TimeseriesCSVIOError:
            abort(422, "Invalid csv file content")
