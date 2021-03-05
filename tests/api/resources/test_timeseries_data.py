"""Timeseries data tests"""
# pylint: disable=invalid-name
import io
import datetime as dt

import pytest

TIMESERIES_URL = '/timeseries-data/'


class TestTimeseriesDataApi:

    @pytest.mark.parametrize(
            'timeseries_data',
            ({"nb_ts": 2, "nb_tsd": 4}, ),
            indirect=True
    )
    def test_timeseries_data_get(self, app, timeseries_data):

        client = app.test_client()

        ts_0_id, _, start_time, end_time = timeseries_data[0]
        ts_1_id, _, _, _ = timeseries_data[1]

        ret = client.get(
            TIMESERIES_URL,
            query_string={
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "timeseries": [ts_0_id, ts_1_id],
            }
        )
        assert ret.status_code == 200
        assert ret.headers['Content-Type'] == "text/csv; charset=utf-8"
        assert ret.headers['Content-Disposition'] == (
            "attachment; filename=timeseries.csv"
        )
        csv_str = ret.data.decode("utf-8")
        assert csv_str == (
            f"Datetime,{ts_0_id},{ts_1_id}\n"
            "2020-01-01 00:00:00,0.0,0.0\n"
            "2020-01-01 01:00:00,1.0,1.0\n"
            "2020-01-01 02:00:00,2.0,2.0\n"
            "2020-01-01 03:00:00,3.0,3.0\n"
        )

    @pytest.mark.parametrize(
            'timeseries_data',
            ({"nb_ts": 2, "nb_tsd": 0}, ),
            indirect=True
    )
    def test_timeseries_data_post(self, app, timeseries_data):

        client = app.test_client()

        start_time = dt.datetime(2020, 1, 1, 0, tzinfo=dt.timezone.utc)
        end_time = dt.datetime(2020, 1, 1, 4, tzinfo=dt.timezone.utc)

        ts_0_id, _, _, _ = timeseries_data[0]
        ts_1_id, _, _, _ = timeseries_data[1]

        # Check there is no data
        ret = client.get(
            TIMESERIES_URL,
            query_string={
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "timeseries": [ts_0_id, ts_1_id],
            }
        )
        assert ret.status_code == 200
        assert ret.headers['Content-Type'] == "text/csv; charset=utf-8"
        assert ret.headers['Content-Disposition'] == (
            "attachment; filename=timeseries.csv"
        )
        csv_str = ret.data.decode("utf-8")
        assert csv_str == "Datetime,1,2\n"

        # Send data
        csv_str = (
            f"Datetime,{ts_0_id},{ts_1_id}\n"
            "2020-01-01T00:00:00+00:00,0,10\n"
            "2020-01-01T01:00:00+00:00,1,11\n"
            "2020-01-01T02:00:00+00:00,2,12\n"
            "2020-01-01T03:00:00+00:00,3,13\n"
        )

        ret = client.post(
            TIMESERIES_URL,
            data={
                "csv_file": (io.BytesIO(csv_str.encode()), 'timeseries.csv')
            }
        )
        assert ret.status_code == 201

        # Check data was written in DB
        ret = client.get(
            TIMESERIES_URL,
            query_string={
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "timeseries": [ts_0_id, ts_1_id],
            }
        )
        assert ret.status_code == 200
        assert ret.headers['Content-Type'] == "text/csv; charset=utf-8"
        assert ret.headers['Content-Disposition'] == (
            "attachment; filename=timeseries.csv"
        )
        csv_str = ret.data.decode("utf-8")
        assert csv_str == (
            f"Datetime,{ts_0_id},{ts_1_id}\n"
            "2020-01-01 00:00:00,0.0,10.0\n"
            "2020-01-01 01:00:00,1.0,11.0\n"
            "2020-01-01 02:00:00,2.0,12.0\n"
            "2020-01-01 03:00:00,3.0,13.0\n"
        )

    @pytest.mark.parametrize(
            'timeseries_data',
            ({"nb_ts": 1, "nb_tsd": 0}, ),
            indirect=True
    )
    @pytest.mark.parametrize(
        "csv_str",
        (
            "",
            "Dummy,\n",
            "Datetime,1324564",
            "Datetime,1\n2020-01-01T00:00:00+00:00",
            "Datetime,1\n2020-01-01T00:00:00+00:00,",
            "Datetime,1\n2020-01-01T00:00:00+00:00,a",
        )
    )
    def test_timeseries_data_post_error(self, app, timeseries_data, csv_str):

        client = app.test_client()

        ret = client.post(
            TIMESERIES_URL,
            data={
                "csv_file": (io.BytesIO(csv_str.encode()), 'timeseries.csv')
            }
        )
        assert ret.status_code == 422
        assert ret.json == {
            "code": 422,
            "status": "Unprocessable Entity",
        }
