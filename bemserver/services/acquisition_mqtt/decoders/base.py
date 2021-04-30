"""MQTT generic payload decoder"""

import abc
import datetime as dt
import sqlalchemy as sqla

from bemserver.core.database import db
from bemserver.core.model import TimeseriesData
from bemserver.services.acquisition_mqtt.exceptions import PayloadDecoderError


class PayloadDecoderBase(abc.ABC):

    name = None

    def __init__(self, topic):
        if topic.payload_decoder != self.__class__.name:
            raise PayloadDecoderError("Incompatible topic payload decoder!")
        self._db_topic = topic

        self.timestamp_last_message = None

    def on_message(self, client, userdata, msg):
        # /!\ note that if message is retained, it can already be in database

        self.timestamp_last_message = dt.datetime.now(dt.timezone.utc)

        try:
            timestamp, value = self._decode(msg.payload)
            self._save_to_db(timestamp, value)
        except PayloadDecoderError:
            # TODO raise or log error
            pass

    @abc.abstractmethod
    def _decode(self, raw_payload):
        raise NotImplementedError

    def _save_to_db(self, timestamp, value):
        tsdata = TimeseriesData(
            timeseries_id=self._db_topic.timeseries_id,
            timestamp=timestamp,
            value=value
        )
        db.session.add(tsdata)
        try:
            db.session.commit()
        except sqla.exc.IntegrityError:
            db.session.rollback()
            # TODO: raise or log error
