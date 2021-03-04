import datetime as dt

import pytest

from dotenv import load_dotenv
from bemserver import create_app
from bemserver.database import db
from bemserver import model


# Load .env file (to load database parameters from development environment)
load_dotenv('.env')


@pytest.fixture
def app():
    application = create_app()
    application.config['TESTING'] = True
    with application.app_context():
        db.drop_all()
        db.create_all()
        model.create_hypertables()
        yield application


@pytest.fixture(params=[{}])
def timeseries_data(request, app):

    param = request.param

    nb_ts = param.get("nb_ts", 1)
    nb_tsd = param.get("nb_tds", 24 * 100)

    ts_l = []

    with app.app_context():

        for i in range(nb_ts):
            ts_i = model.Timeseries(
                name=f"Timeseries {i}",
                description=f"Test timeseries #{i}",
            )
            db.session.add(ts_i)

            start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
            for i in range(nb_tsd):
                timestamp = start_dt + dt.timedelta(hours=i)
                db.session.add(
                    model.TimeseriesData(
                        timestamp=timestamp,
                        timeseries=ts_i,
                        value=i
                    )
                )

            ts_l.append(ts_i)

        db.session.commit()

        return [
            (ts.id, nb_tsd, start_dt, start_dt + dt.timedelta(hours=nb_tsd))
            for ts in ts_l
        ]
