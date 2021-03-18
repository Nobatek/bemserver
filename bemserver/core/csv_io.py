"""Timeseries CSV I/O"""
import io
import csv

import psycopg2
from sqlalchemy.sql.expression import func
import pandas as pd

from .database import db
from .exceptions import TimeseriesCSVIOError
from .model import Timeseries, TimeseriesData


class TimeseriesCSVIO:

    @staticmethod
    def import_csv(csv_file):
        """Import CSV file

        :param srt|TextIOBase csv_file: CSV as string or text stream
        """
        # If input is not a text stream, then it is a plain string
        # Make it an iterator
        if not isinstance(csv_file, io.TextIOBase):
            csv_file = csv_file.splitlines()

        reader = csv.reader(csv_file)

        try:
            header = next(reader)
        except StopIteration as exc:
            raise TimeseriesCSVIOError('Missing headers line') from exc
        if header[0] != "Datetime":
            raise TimeseriesCSVIOError('First column must be "Datetime"')
        try:
            ts_ids = [
                db.session.query(Timeseries).get(col).id
                for col in header[1:]
            ]
        except AttributeError as exc:
            raise TimeseriesCSVIOError('Unknown timeseries ID') from exc

        query = (
            "INSERT INTO timeseries_data "
            "(timestamp, timeseries_id, value) "
            "VALUES %s "
            "ON CONFLICT DO NOTHING"
        )

        datas = []
        for row in reader:
            try:
                timestamp = row[0]
                datas.extend([
                    (timestamp, ts_id, row[col+1])
                    for col, ts_id in enumerate(ts_ids)
                ])
            except IndexError as exc:
                raise TimeseriesCSVIOError('Missing column') from exc

        with db.raw_connection() as conn, conn.cursor() as cur:
            try:
                psycopg2.extras.execute_values(cur, query, datas)
                conn.commit()
            # TODO: filter server and client errors (constraint violation)
            except psycopg2.Error as exc:
                raise TimeseriesCSVIOError('Error writing to DB') from exc

    @staticmethod
    def export_csv(start_dt, end_dt, timeseries):
        """Export timeseries data as CSV file

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)

        Returns csv as a string.
        """
        data = db.session.query(
            func.timezone("UTC", TimeseriesData.timestamp),
            TimeseriesData.timeseries_id,
            TimeseriesData.value,
        ).filter(
            TimeseriesData.timeseries_id.in_(timeseries)
        ).filter(
            start_dt <= TimeseriesData.timestamp
        ).filter(
            TimeseriesData.timestamp < end_dt
        ).all()

        data_df = (
            pd.DataFrame(data, columns=('Datetime', 'tsid', 'value'))
            .set_index("Datetime")
        )
        data_df.index = pd.DatetimeIndex(data_df.index).tz_localize('UTC')
        data_df = data_df.pivot(columns='tsid', values='value')

        # Add missing columns, in query order
        for idx, ts_id in enumerate(timeseries):
            if ts_id not in data_df:
                data_df.insert(idx, ts_id, None)

        # Specify ISO 8601 manually
        # https://github.com/pandas-dev/pandas/issues/27328
        return data_df.to_csv(date_format='%Y-%m-%dT%H:%M:%S%z')

    @staticmethod
    def export_csv_bucket(start_dt, end_dt, timeseries, bucket_width):
        """Bucket timeseries data and export as CSV file

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)

        Returns csv as a string.
        """
        query = (
            "SELECT time_bucket(%s, timestamp) AT TIME ZONE 'UTC'"
            "  AS bucket, timeseries_id, avg(value) "
            "FROM timeseries_data "
            "WHERE timeseries_id IN %s "
            "  AND timestamp >= %s AND timestamp < %s "
            "GROUP BY bucket, timeseries_id "
            "ORDER BY bucket;"
        )
        params = (bucket_width, timeseries, start_dt, end_dt)

        with db.raw_connection() as conn, conn.cursor() as cur:
            try:
                cur.execute(query, params)
                data = cur.fetchall()
            except psycopg2.Error as exc:
                raise exc

        data_df = (
            pd.DataFrame(data, columns=('Datetime', 'tsid', 'value'))
            .set_index("Datetime")
        )
        data_df.index = pd.DatetimeIndex(data_df.index).tz_localize('UTC')
        data_df = data_df.pivot(columns='tsid', values='value')

        # Add missing columns, in query order
        for idx, ts_id in enumerate(timeseries):
            if ts_id not in data_df:
                data_df.insert(idx, ts_id, None)

        # Specify ISO 8601 manually
        # https://github.com/pandas-dev/pandas/issues/27328
        return data_df.to_csv(date_format='%Y-%m-%dT%H:%M:%S%z')


tscsvio = TimeseriesCSVIO()
