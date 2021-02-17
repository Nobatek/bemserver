"""Timeseries data tests"""
# pylint: disable=invalid-name


TIMESERIES_URL = '/timeseries-data/'


class TestTimeseriesDataApi:

    def test_timeseries_data_get(self, app, timeseries_data):

        client = app.test_client()

        ts_1_id, nb_samples, start_time, end_time = timeseries_data[0]

        # GET list
        ret = client.get(
            f"{TIMESERIES_URL}{ts_1_id}",
            query_string={
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            }
        )
        assert ret.status_code == 200
        assert len(ret.json) == nb_samples
        assert ret.json[0] == {
            "timestamp": "2020-01-01T00:00:00",
            "value": 0,
        }
