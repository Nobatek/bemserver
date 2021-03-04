"""Timeseries CSV I/O tests"""
# pylint: disable=invalid-name
import io
import datetime as dt

import pytest

from sqlalchemy.sql.expression import func

from bemserver.model import TimeseriesData
from bemserver.csv_io import tscsvio
from bemserver.database import db
from bemserver.exceptions import TimeseriesCSVIOError


class TestTimeseriesCSVIO:

    @pytest.mark.parametrize(
            'timeseries_data',
            ({"nb_ts": 2, "nb_tds": 0}, ),
            indirect=True
    )
    @pytest.mark.parametrize('mode', ('binary', 'text'))
    def test_timeseries_csv_io_import_csv(self, timeseries_data, mode):

        ts_0_id, _, _, _ = timeseries_data[0]
        ts_1_id, _, _, _ = timeseries_data[1]

        assert not TimeseriesData.query.all()

        csv_s = (
            f"Datetime,{ts_0_id},{ts_1_id}\n"
            "2020-01-01T00:00:00+00:00,0,10\n"
            "2020-01-01T01:00:00+00:00,1,11\n"
            "2020-01-01T02:00:00+00:00,2,12\n"
            "2020-01-01T03:00:00+00:00,3,13\n"
        )

        if mode == "text":
            tscsvio.import_csv(io.StringIO(csv_s))
        else:
            csv_b = csv_s.encode("utf-8")
            tscsvio.import_csv(io.BytesIO(csv_b))

        data = db.session.query(
            func.timezone("UTC", TimeseriesData.timestamp).label("timestamp"),
            TimeseriesData.timeseries_id,
            TimeseriesData.value,
        ).order_by(
            TimeseriesData.timeseries_id,
            TimeseriesData.timestamp,
        ).all()

        timestamps = [dt.datetime(2020, 1, 1, i) for i in range(4)]

        expected = [
                (timestamp, ts_0_id, float(idx))
                for idx, timestamp in enumerate(timestamps)
            ] + [
                (timestamp, ts_1_id, float(idx) + 10)
                for idx, timestamp in enumerate(timestamps)
            ]

        assert data == expected

    @pytest.mark.parametrize(
            'timeseries_data',
            ({"nb_ts": 1, "nb_tds": 0}, ),
            indirect=True
    )
    @pytest.mark.parametrize(
        "csv_file",
        (
            "",
            "Dummy,\n",
            "Datetime,1324564",
            "Datetime,1\n2020-01-01T00:00:00+00:00",
            "Datetime,1\n2020-01-01T00:00:00+00:00,",
            "Datetime,1\n2020-01-01T00:00:00+00:00,a",
        )
    )
    @pytest.mark.usefixtures("timeseries_data")
    def test_timeseries_csv_io_import_csv_error(self, csv_file):
        with pytest.raises(TimeseriesCSVIOError):
            tscsvio.import_csv(io.StringIO(csv_file))
