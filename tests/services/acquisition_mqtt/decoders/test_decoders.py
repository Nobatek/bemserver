"""Payload decoders tests"""

import pytest
import json
import time
import datetime as dt
import sqlalchemy as sqla

from bemserver.core.database import db
from bemserver.core.model import TimeseriesData
from bemserver.services.acquisition_mqtt import decoders
from bemserver.services.acquisition_mqtt.exceptions import (
    PayloadDecoderError, PayloadDecoderNotFoundError)

from tests.services.acquisition_mqtt.conftest import PayloadDecoderCustom


class TestPayloadDecoders:

    def test_payload_decoder_is_registered(self):

        assert not decoders.is_payload_decoder_registered("inexistant_decoder")
        assert decoders.is_payload_decoder_registered(
            decoders.PayloadDecoderBEMServer.name)

    def test_payload_decoder_get_cls(self):

        payload_decoder_cls = decoders.get_payload_decoder(
            decoders.PayloadDecoderBEMServer.name)
        assert payload_decoder_cls == decoders.PayloadDecoderBEMServer

        with pytest.raises(PayloadDecoderNotFoundError):
            decoders.get_payload_decoder("inexistant_decoder")

    def test_payload_decoder_register(self):

        with pytest.raises(PayloadDecoderNotFoundError):
            decoders.get_payload_decoder(PayloadDecoderCustom.name)

        decoders.register_payload_decoder(PayloadDecoderCustom)

        payload_decoder_cls = decoders.get_payload_decoder(
            PayloadDecoderCustom.name)
        assert payload_decoder_cls == PayloadDecoderCustom

        with pytest.raises(PayloadDecoderError):
            decoders.register_payload_decoder(TimeseriesData)

    def test_payload_decoder_topic_decoder_mismatch(
            self, database, topic, decoder_custom_cls):

        assert topic.payload_decoder != decoder_custom_cls.name
        with pytest.raises(PayloadDecoderError):
            PayloadDecoderCustom(topic)

    def test_payload_decoder_decode(self, topic, decoder_custom_cls):

        bemserver_decoder_cls = decoders.get_payload_decoder("bemserver")
        bemserver_decoder = bemserver_decoder_cls(topic)
        ts_now = dt.datetime.now(dt.timezone.utc)
        payload_to_decode = {
            "ts": ts_now.isoformat(),
            "value": 66.6,
        }
        ts, value = bemserver_decoder._decode(json.dumps(payload_to_decode))
        assert ts == ts_now
        assert value == payload_to_decode["value"]

        topic.payload_decoder = decoder_custom_cls.name
        custom_decoder = decoder_custom_cls(topic)
        ts_before_decode = dt.datetime.now(dt.timezone.utc)
        payload_to_decode = bytes("754369 seconds", "utf-8")
        timestamp, value = custom_decoder._decode(payload_to_decode)
        ts_after_decode = dt.datetime.now(dt.timezone.utc)
        assert ts_before_decode < timestamp < ts_after_decode
        assert value == 754369

    def test_payload_decoder_on_message(self, database, topic, publisher):

        # At first there is no timeseries data.
        stmt = sqla.select(TimeseriesData).filter(
            TimeseriesData.timeseries_id == topic.timeseries_id
        )
        tsdatas = db.session.execute(stmt).all()
        assert len(tsdatas) == 0

        # Connect a subscriber to receive messages.
        topic.subscriber.connect()
        time.sleep(0.2)
        assert topic.subscriber._client.is_connected()
        topic.subscriber.subscribe_all()
        time.sleep(0.5)
        topic.subscriber.disconnect()

        # A retained message has been received and stored as timeseries in DB.
        tsdatas = db.session.execute(stmt).all()
        assert len(tsdatas) == 1
        tsdata = tsdatas[0][0]
        assert tsdata.timeseries_id == topic.timeseries_id
        assert tsdata.timestamp == dt.datetime(
            2021, 4, 27, 16, 5, 11, tzinfo=dt.timezone.utc)
        assert tsdata.value == 42
