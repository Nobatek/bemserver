"""Model"""
from bemserver.database import db

from .timeseries import Timeseries  # noqa
from .timeseries_data import TimeseriesData  # noqa


HYPERTABLES = {
    'timeseries_data': 'timestamp',
}


def create_hypertables():
    """Create Timescale hypertables"""
    with db.raw_connection() as conn, conn.cursor() as cur:
        for table, column in HYPERTABLES.items():
            query = f"SELECT create_hypertable('{table}', '{column}');"
            cur.execute(query)
