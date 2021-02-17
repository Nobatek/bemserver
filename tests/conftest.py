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
    return application


@pytest.fixture
def timeseries_data(app):
    with app.app_context():
        ts_1 = model.Timeseries(
            name="Timeseries 1",
            description="Test timeseries #1",
        )
        db.session.add(ts_1)

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        nb_samples = 24 * 100
        for i in range(nb_samples):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                model.TimeseriesData(
                    timestamp=timestamp,
                    timeseries=ts_1,
                    value=i
                )
            )

        db.session.commit()
        return [
            (
                ts_1.id,
                nb_samples,
                start_dt,
                start_dt + dt.timedelta(hours=nb_samples)
            )
        ]
