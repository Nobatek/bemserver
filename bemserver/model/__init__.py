"""Model"""
from bemserver.database import rc

from .timeseries import Timeseries  # noqa
from .timeseries_data import TimeseriesData  # noqa


HYPERTABLES = {
    'timeseries_data': 'timestamp',
}


def create_hypertables():
    """Create Timescale hypertables"""
    with rc.connection() as conn:
        cur = conn.cursor()
        for table, column in HYPERTABLES.items():
            query = f"SELECT create_hypertable('{table}', '{column}');"
            cur.execute(query)
