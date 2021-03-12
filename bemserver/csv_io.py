"""Timeseries CSV I/O"""
import io
import csv

import psycopg2
from sqlalchemy.sql.expression import func
import pandas as pd

from .database import db, rc
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
                Timeseries.query.get(col).id
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

        with rc.connection() as conn, conn.cursor() as cur:
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
        data_df = data_df.pivot(columns='tsid', values='value')

        # Add missing columns, in query order
        for idx, ts_id in enumerate(timeseries):
            if ts_id not in data_df:
                data_df.insert(idx, ts_id, None)

        return data_df.to_csv()


tscsvio = TimeseriesCSVIO()
