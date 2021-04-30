"""Topic tests"""

import pytest
import datetime as dt
import sqlalchemy as sqla

from bemserver.core.database import db
from bemserver.core.model import Timeseries
from bemserver.services.acquisition_mqtt.model import Topic


class TestTopicModel:

    def test_topic_crud(self, database, subscriber, decoder_custom_cls):

        assert Topic.get_by_id(None) is None
        assert Topic.get_by_id(1) is None

        ts = Timeseries(
            name="Timeseries 0", description="Test timeseries #0")
        db.session.add(ts)
        db.session.commit()

        topic = Topic(
            name="$SYS/broker/uptime",
            payload_decoder=decoder_custom_cls.name,
            timeseries_id=ts.id, subscriber_id=subscriber.id)
        assert topic.id is None
        topic.save()
        assert topic.id is not None
        assert topic.qos == 1
        assert topic.is_enabled
        assert not topic.is_subscribed
        assert topic.timestamp_last_subscription is None
        assert topic.timeseries == ts
        assert topic.subscriber == subscriber
        assert topic.payload_decoder_cls == decoder_custom_cls

        assert Topic.get_by_id(topic.id) == topic

        ts_now = dt.datetime.now(dt.timezone.utc)
        topic.update_subscription(True)
        assert topic.is_subscribed
        assert topic.timestamp_last_subscription is not None
        assert topic.timestamp_last_subscription > ts_now
        ts_last_sub = topic.timestamp_last_subscription

        topic.update_subscription(False)
        assert not topic.is_subscribed
        assert topic.timestamp_last_subscription == ts_last_sub

        # Integrity error while deleting a timeseries referenced in a topic.
        db.session.delete(ts)
        with pytest.raises(sqla.exc.IntegrityError):
            db.session.commit()
        db.session.rollback()

        topic.delete()
        assert Topic.get_by_id(topic.id) is None

        topic.delete()  # try to delete again
        assert Topic.get_by_id(topic.id) is None

        topic.save()
        assert Topic.get_by_id(topic.id) is not None

        topic.qos = 666
        with pytest.raises(ValueError) as exc:
            topic._verify_consistency()
            assert str(exc) == "Invalid QoS level!"

        topic.qos = 2
        topic.payload_decoder = "inexistant_decoder"
        with pytest.raises(ValueError) as exc:
            topic._verify_consistency()
            assert "payload decoder is not registered!" in str(exc)

    def test_topic_get_list(self, database, subscriber):

        rows = Topic.get_list(subscriber.id)
        assert len(rows) == 0

        topics = []
        for i in range(2):
            ts = Timeseries(
                name=f"Timeseries {i}", description=f"Test timeseries #{i}")
            db.session.add(ts)
            db.session.commit()
            topic = Topic(
                name=f"topic/{i}", timeseries_id=ts.id,
                subscriber_id=subscriber.id)
            topic.save()
            topics.append(topic)

        rows = Topic.get_list(subscriber.id)
        assert len(rows) == 2
        assert rows[0][0].id == topics[0].id
        assert rows[1][0].id == topics[1].id

        topics[1].is_enabled = False
        topics[1].save()

        rows = Topic.get_list(subscriber.id, is_enabled=True)
        assert len(rows) == 1
        assert rows[0][0].id == topics[0].id

        rows = Topic.get_list(subscriber.id, is_enabled=False)
        assert len(rows) == 1
        assert rows[0][0].id == topics[1].id

        rows = Topic.get_list(42)
        assert len(rows) == 0
