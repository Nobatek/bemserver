"""Service tests"""

import time
import pytest
import sqlalchemy as sqla

from bemserver.core.database import db
from bemserver.core.model import TimeseriesData
from bemserver.services.acquisition_mqtt.model import TopicBySubscriber
from bemserver.services.acquisition_mqtt.service import Service
from bemserver.services.acquisition_mqtt.exceptions import ServiceError


class TestServiceMQTT:

    def test_service_mqtt_set_db_url(self, db_url, tmpdir):

        svc = Service(str(tmpdir))
        assert db.engine is None
        svc.set_db_url(db_url)
        assert db.engine is not None
        assert str(db.engine.url) == db_url

    def test_service_mqtt_set_db_url_already_done(
            self, db_url, tmpdir, database):

        svc = Service(str(tmpdir))
        assert db.engine is not None
        assert str(db.engine.url) == db_url
        svc.set_db_url(db_url)
        assert db.engine is not None
        assert str(db.engine.url) == db_url

    def test_service_mqtt_run(
            self, tmpdir, database, subscriber, topic, publisher):

        assert topic.is_enabled

        assert subscriber.is_enabled
        assert not subscriber.is_connected

        topic_by_subscriber = topic.add_subscriber(subscriber.id)
        assert not topic_by_subscriber.is_subscribed

        # No timeseries data yet.
        stmt = sqla.select(TimeseriesData)
        for topic_link in topic.links:
            stmt = stmt.filter(
                TimeseriesData.timeseries_id == topic_link.timeseries_id
            )
        stmt = stmt.order_by(
            TimeseriesData.timestamp
        )
        rows = db.session.execute(stmt).all()
        assert len(rows) == 0

        svc = Service(str(tmpdir))
        assert not svc.is_running
        assert svc._running_subscribers == []
        svc.run()
        assert svc.is_running
        assert subscriber.is_connected
        assert topic_by_subscriber.is_subscribed
        assert len(svc._running_subscribers) == 1
        assert subscriber.id in [x.id for x in svc._running_subscribers]

        # Waiting for messages.
        time.sleep(1)

        svc.stop()
        assert not svc.is_running
        assert not subscriber.is_connected
        assert not topic_by_subscriber.is_subscribed
        assert svc._running_subscribers == []

        # At least one timeseries data received.
        rows = db.session.execute(stmt).all()
        assert len(rows) >= 1

    def test_service_mqtt_run_tls(
            self, tmpdir, database, subscriber_tls, topic, publisher):

        assert topic.is_enabled

        assert subscriber_tls.is_enabled
        assert not subscriber_tls.is_connected

        topic_by_subscriber = topic.add_subscriber(subscriber_tls.id)
        assert not topic_by_subscriber.is_subscribed

        # No timeseries data yet.
        stmt = sqla.select(TimeseriesData)
        for topic_link in topic.links:
            stmt = stmt.filter(
                TimeseriesData.timeseries_id == topic_link.timeseries_id
            )
        stmt = stmt.order_by(
            TimeseriesData.timestamp
        )
        rows = db.session.execute(stmt).all()
        assert len(rows) == 0

        svc = Service(str(tmpdir))
        assert not svc.is_running
        assert svc._running_subscribers == []
        svc.run()
        assert svc.is_running
        assert subscriber_tls.is_connected
        assert topic_by_subscriber.is_subscribed
        assert len(svc._running_subscribers) == 1
        assert subscriber_tls.id in [x.id for x in svc._running_subscribers]

        # Waiting for messages.
        time.sleep(1)

        svc.stop()
        assert not svc.is_running
        assert not subscriber_tls.is_connected
        assert not topic_by_subscriber.is_subscribed
        assert svc._running_subscribers == []

        # At least one timeseries data received.
        rows = db.session.execute(stmt).all()
        assert len(rows) >= 1

    def test_service_mqtt_nothing_to_do(self, db_url, tmpdir):
        svc = Service(str(tmpdir))
        svc.set_db_url(db_url)
        db.setup_tables()  # init database (for this test only)
        with pytest.raises(ServiceError) as exc:
            svc.run()
            assert str(exc) == (
                "No subscribers available to run MQTT acquisition!")
        assert svc._running_subscribers == []
