"""Topic tests"""

import pytest
import datetime as dt
import sqlalchemy as sqla

from bemserver.core.database import db
from bemserver.core.model import Timeseries
from bemserver.services.acquisition_mqtt.model import Topic, TopicLink


class TestTopicModel:

    def test_topic_crud(self, database, subscriber, decoder_mosquitto_uptime):

        decoder_mosquitto_uptime_cls, decoder = decoder_mosquitto_uptime

        assert Topic.get_by_id(None) is None
        assert Topic.get_by_id(1) is None

        topic = Topic(
            name="$SYS/broker/uptime", payload_decoder_id=decoder.id,
            subscriber_id=subscriber.id)
        assert topic.id is None
        topic.save()
        assert topic.id is not None
        assert topic.qos == 1
        assert topic.is_enabled
        assert not topic.is_subscribed
        assert topic.timestamp_last_subscription is None
        assert topic.subscriber == subscriber
        assert topic.payload_decoder == decoder
        assert topic.payload_decoder_cls == decoder_mosquitto_uptime_cls
        assert isinstance(
            topic.payload_decoder_instance, decoder_mosquitto_uptime_cls)
        assert topic.links == []

        assert Topic.get_by_id(topic.id) == topic

        for payload_field in decoder.fields:
            ts = Timeseries(name=f"Timeseries {payload_field.name}")
            db.session.add(ts)
            db.session.commit()
            topic_link = topic.add_link(payload_field.id, ts.id)
            assert topic_link.topic == topic
            assert topic_link.payload_field == payload_field
            assert topic_link.timeseries == ts

        assert len(topic.links) == len(decoder.fields)
        assert [x.payload_field.name for x in topic.links] == [
            x.name for x in decoder.fields]

        ts_now = dt.datetime.now(dt.timezone.utc)
        topic.update_subscription(True)
        assert topic.is_subscribed
        assert topic.timestamp_last_subscription is not None
        assert topic.timestamp_last_subscription > ts_now
        ts_last_sub = topic.timestamp_last_subscription

        topic.update_subscription(False)
        assert not topic.is_subscribed
        assert topic.timestamp_last_subscription == ts_last_sub

        # Integrity error if deleting a payload decoder referenced in a topic.
        with pytest.raises(sqla.exc.IntegrityError):
            decoder.delete()

        # Deleting a topic also deletes its links in cascade.
        topic_id = topic.id
        topic.delete()
        assert Topic.get_by_id(topic.id) is None
        stmt = sqla.select(TopicLink).filter(TopicLink.topic_id == topic_id)
        rows = db.session.execute(stmt).all()
        assert len(rows) == 0

        topic.delete()  # try to delete again
        assert Topic.get_by_id(topic.id) is None

        topic.save()
        assert Topic.get_by_id(topic.id) is not None
        assert len(topic.links) == 0

        topic.qos = 666
        with pytest.raises(ValueError) as exc:
            topic._verify_consistency()
            assert str(exc) == "Invalid QoS level!"

        topic.qos = 2
        topic.payload_decoder_id = 666
        with pytest.raises(sqla.exc.IntegrityError):
            topic.save()

        # Remove topic link.
        topic.add_link(decoder.fields[0].id, ts.id)
        assert len(topic.links) == 1
        topic.remove_link(666, ts.id)
        assert len(topic.links) == 1
        topic.remove_link(decoder.fields[0].id, 666)
        assert len(topic.links) == 1
        topic.remove_link(decoder.fields[0].id, ts.id)
        assert len(topic.links) == 0

        # Deleting payload field also removes topic links concerned in cascade.
        ts = Timeseries(name="Timeseries test")
        db.session.add(ts)
        db.session.commit()
        topic.add_link(decoder.fields[0].id, ts.id)
        assert len(topic.links) == 1
        decoder.fields[0].delete()
        assert len(topic.links) == 0

    def test_topic_get_list(
            self, database, subscriber, decoder_mosquitto_uptime):

        _, decoder = decoder_mosquitto_uptime

        rows = Topic.get_list(subscriber.id)
        assert len(rows) == 0

        topics = []
        for i in range(2):
            topic = Topic(
                name=f"topic/{i}", payload_decoder_id=decoder.id,
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


class TestTopicLinkModel:

    def test_topic_link_crud(
            self, database, subscriber, decoder_mosquitto_uptime):

        _, decoder = decoder_mosquitto_uptime

        topic = Topic(
            name="$SYS/broker/uptime", payload_decoder_id=decoder.id,
            subscriber_id=subscriber.id)
        topic.save()

        assert topic.links == []

        for payload_field in decoder.fields:
            ts = Timeseries(name=f"Timeseries {payload_field.name}")
            db.session.add(ts)
            db.session.commit()
            topic_link = TopicLink(
                topic_id=topic.id, payload_field_id=payload_field.id,
                timeseries_id=ts.id)
            topic_link.save()
            assert topic_link.topic == topic
            assert topic_link.payload_field == payload_field
            assert topic_link.timeseries == ts

        assert len(topic.links) == len(decoder.fields)
        assert [x.payload_field.name for x in topic.links] == [
            x.name for x in decoder.fields]

        # Verify unique constraints between:
        #   - topic and payload field
        #   - topic and timeseries
        topic_link = TopicLink(
            topic_id=topic.id,
            payload_field_id=topic.links[0].payload_field_id,
            timeseries_id=topic.links[0].timeseries_id)
        with pytest.raises(sqla.exc.IntegrityError):
            topic_link.save()

        payload_field_test = decoder.add_field("test")
        topic_link.payload_field_id = payload_field_test.id
        with pytest.raises(sqla.exc.IntegrityError):
            topic_link.save()

        ts_test = Timeseries(name="Timeseries test")
        db.session.add(ts_test)
        db.session.commit()
        topic_link.payload_field_id = topic.links[0].payload_field_id
        topic_link.timeseries_id = ts_test.id
        with pytest.raises(sqla.exc.IntegrityError):
            topic_link.save()

        topic_link.payload_field_id = payload_field_test.id
        topic_link.save()
        assert len(topic.links) == 2

        topic_link.delete()
        assert len(topic.links) == 1
