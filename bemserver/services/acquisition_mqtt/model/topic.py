"""MQTT topic"""

import datetime as dt
import sqlalchemy as sqla

from bemserver.core.database import Base, db
from bemserver.services.acquisition_mqtt import decoders


class TopicByBroker(Base):
    """Describes the association between Topic and Broker.

    :param int topic_id: Relation to a topic unique ID.
    :param broker_id: Relation to a broker unique ID.
    :param bool is_enabled: (optional, default True)
        Active/deactivate the link between topic and broker.
    """
    __tablename__ = "mqtt_topic_by_broker"
    __table_args__ = (
        sqla.PrimaryKeyConstraint("topic_id", "broker_id"),
    )

    topic_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("mqtt_topic.id", ondelete="CASCADE"),
        nullable=False,
    )
    broker_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("mqtt_broker.id"),
        nullable=False,
    )
    is_enabled = sqla.Column(sqla.Boolean, nullable=False, default=True)

    def __repr__(self):
        str_fields = ", ".join([
            f"{x.name}={getattr(self, x.name)}" for x in self.__table__.columns
        ])
        return f"<{self.__table__.name}>({str_fields})"

    def save(self):
        """Write the item data to the database."""
        db.session.add(self)
        try:
            db.session.commit()
        except sqla.exc.IntegrityError as exc:
            db.session.rollback()
            raise exc

    def delete(self):
        """Delete the item from the database."""
        db.session.delete(self)
        db.session.commit()

    @classmethod
    def get(cls, topic_id, broker_id):
        """Find an existing association, in database, between topic and broker.

        :param int topic_id: Unique ID of a topic.
        :param int broker_id: Unique ID of a broker.
        :returns TopicByBroker: Instance found of `TopicByBroker`.
        """
        stmt = sqla.select(cls)
        stmt = stmt.filter(cls.topic_id == topic_id)
        stmt = stmt.filter(cls.broker_id == broker_id)
        try:
            return db.session.execute(stmt).one()[0]
        except sqla.exc.NoResultFound:
            return None


class TopicBySubscriber(Base):
    """Describes the association between Topic and Subscriber.

    :param int topic_id: Relation to a topic unique ID.
    :param subscriber_id: Relation to a subscriber unique ID.
    :param int qos: (default 1)
        Level of Quality of Service (QoS) used for this topic/subscriber.
    :param bool is_subscribed: (optional, default False)
        This allows to known the last topic subscription status.
    :param datetime timestamp_last_subscription: (optional, default None)
        Field auto-updated by `update_subscription` method.
        This allows to known the last topic subscription timestamp.
    :param bool is_enabled: (optional, default True)
        Active/deactivate the link between topic and subscriber.
    """
    __tablename__ = "mqtt_topic_by_subscriber"
    __table_args__ = (
        sqla.PrimaryKeyConstraint("topic_id", "subscriber_id"),
    )

    topic_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("mqtt_topic.id", ondelete="CASCADE"),
        nullable=False,
    )
    subscriber_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("mqtt_subscriber.id"),
        nullable=False,
    )
    is_enabled = sqla.Column(sqla.Boolean, nullable=False, default=True)

    # TODO: see if an overrided qos will be needed here or not
    # qos = sqla.Column(sqla.Integer, nullable=False, default=1)
    is_subscribed = sqla.Column(sqla.Boolean, nullable=False, default=False)
    timestamp_last_subscription = sqla.Column(sqla.DateTime(timezone=True))

    def __repr__(self):
        str_fields = ", ".join([
            f"{x.name}={getattr(self, x.name)}" for x in self.__table__.columns
        ])
        return f"<{self.__table__.name}>({str_fields})"

    def update_subscription(self, is_subscribed):
        """Update topic subscription status for a subscriber.

        :param bool is_subscribed: Whether topic has been subscribed or not.
        """
        self.is_subscribed = is_subscribed
        if is_subscribed:
            # Only update this field value at subscription time.
            self.timestamp_last_subscription = dt.datetime.now(dt.timezone.utc)
        self.save()

    def save(self):
        """Write the item data to the database."""
        db.session.add(self)
        try:
            db.session.commit()
        except sqla.exc.IntegrityError as exc:
            db.session.rollback()
            raise exc

    def delete(self):
        """Delete the item from the database."""
        db.session.delete(self)
        db.session.commit()

    @classmethod
    def get(cls, topic_id, subscriber_id):
        """Find an existing association between topic and subscriber.

        :param int topic_id: Unique ID of a topic.
        :param int subscriber_id: Unique ID of a subscriber.
        :returns TopicBySubscriber: Instance found of `TopicBySubscriber`.
        """
        stmt = sqla.select(cls)
        stmt = stmt.filter(cls.topic_id == topic_id)
        stmt = stmt.filter(cls.subscriber_id == subscriber_id)
        try:
            return db.session.execute(stmt).one()[0]
        except sqla.exc.NoResultFound:
            return None


class TopicLink(Base):
    """Describers the links between topic, payload fields and timeseries.

    :param int topic_id: Relation to a topic unique ID.
    :param int payload_field_id: Relation to a payload field unique ID.
    :param int timeseries_id: Relation to a timeseries unique ID.
    """
    __tablename__ = "mqtt_topic_link"
    __table_args__ = (
        sqla.PrimaryKeyConstraint(
            "topic_id", "payload_field_id", "timeseries_id"),
        sqla.UniqueConstraint("topic_id", "payload_field_id"),
        sqla.UniqueConstraint("topic_id", "timeseries_id"),
    )

    topic_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("mqtt_topic.id", ondelete="CASCADE"),
        nullable=False,
    )
    payload_field_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("mqtt_payload_field.id", ondelete="CASCADE"),
        nullable=False,
    )
    timeseries_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("timeseries.id"),
        nullable=False,
    )

    topic = sqla.orm.relationship("Topic", back_populates="links")
    payload_field = sqla.orm.relationship(
        "PayloadField", back_populates="topic_links")
    timeseries = sqla.orm.relationship("Timeseries", backref=__tablename__)

    def __repr__(self):
        str_fields = ", ".join([
            f"{x.name}={getattr(self, x.name)}" for x in self.__table__.columns
        ])
        return f"<{self.__table__.name}>({str_fields})"

    def save(self):
        """Write the item data to the database."""
        db.session.add(self)
        try:
            db.session.commit()
        except sqla.exc.IntegrityError as exc:
            db.session.rollback()
            raise exc

    def delete(self):
        """Delete the item from the database."""
        db.session.delete(self)
        db.session.commit()

    @classmethod
    def get(cls, payload_field_id, timeseries_id):
        """Find a topic link in database, from its relation to a payload
        decoder field and a timeseries.

        :param int payload_field_id:
            Unique ID of the payload decoder field related to the link.
        :param int timeseries_id: Unique ID of a timeseries.
        :returns TopicLink: Instance found of `TopicLink`.
        """
        stmt = sqla.select(cls)
        stmt = stmt.filter(cls.payload_field_id == payload_field_id)
        stmt = stmt.filter(cls.timeseries_id == timeseries_id)
        try:
            return db.session.execute(stmt).one()[0]
        except sqla.exc.NoResultFound:
            return None


class Topic(Base):
    """Describes how topics should be subscribed and how payload is decoded.

    :param str name: Topic name (for example: "sensors/temperature/1").
    :param int qos: (default 1)
        Level of Quality of Service (QoS) used for this topic.
    :param str description: (optional, default None)
        Text to describe the topic.
    :param int payload_decoder_id: Relation to a payload decoder unique ID.
    :param bool is_enabled: (optional, default True)
        Active/deactivate the topic.
    """
    __tablename__ = "mqtt_topic"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(250), unique=True, nullable=False)
    qos = sqla.Column(sqla.Integer, nullable=False, default=1)
    description = sqla.Column(sqla.String(250))
    payload_decoder_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("mqtt_payload_decoder.id"),
        nullable=False,
    )
    is_enabled = sqla.Column(sqla.Boolean, nullable=False, default=True)

    payload_decoder = sqla.orm.relationship(
        "PayloadDecoder", back_populates="topics")
    links = sqla.orm.relationship(
        "TopicLink", back_populates="topic", cascade="all,delete",
        passive_deletes=True)

    brokers = sqla.orm.relationship(
        "Broker", secondary=TopicByBroker.__tablename__,
        back_populates="topics", cascade="all,delete", passive_deletes=True)
    subscribers = sqla.orm.relationship(
        "Subscriber", secondary=TopicBySubscriber.__tablename__,
        back_populates="topics", cascade="all,delete", passive_deletes=True)

    @property
    def payload_decoder_cls(self):
        if self._payload_decoder_cls is None:
            self._payload_decoder_cls = decoders.get_payload_decoder_cls(
                self.payload_decoder.name)
        return self._payload_decoder_cls

    @property
    def payload_decoder_instance(self):
        if self._payload_decoder_instance is None:
            self._payload_decoder_instance = self.payload_decoder_cls(self)
        return self._payload_decoder_instance

    @property
    def _db_state(self):
        return sqla.orm.util.object_state(self)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._payload_decoder_cls = None
        self._payload_decoder_instance = None

    def __repr__(self):
        str_fields = ", ".join([
            f"{x.name}={getattr(self, x.name)}" for x in self.__table__.columns
        ])
        return f"<{self.__table__.name}>({str_fields})"

    # TODO: add link from payload field name?
    def add_link(self, payload_field_id, timeseries_id):
        """Add a payload field/timeseries link to this topic.

        :param int payload_field_id:
            Unique field ID from the payload decoder used for this topic.
        :param int timeseries_id: Unique ID of a timeseries to link with.
        :raises sqla.exc.IntegrityError: When topic link integrity is broken.
        """
        topic_link = TopicLink(
            topic_id=self.id, payload_field_id=payload_field_id,
            timeseries_id=timeseries_id)
        topic_link.save()
        return topic_link

    # TODO: remove link from payload field name?
    def remove_link(self, payload_field_id, timeseries_id):
        """Remove a payload field/timeseries link from this topic.

        :param int payload_field_id:
            Unique field ID from the payload decoder used for this topic.
        :param int timeseries_id: Unique ID of a timeseries to link with.
        """
        topic_link = TopicLink.get(payload_field_id, timeseries_id)
        if topic_link in self.links:
            topic_link.delete()

    def add_broker(self, broker_id):
        topic_by_broker = TopicByBroker(topic_id=self.id, broker_id=broker_id)
        topic_by_broker.save()
        return topic_by_broker

    def remove_broker(self, broker_id):
        topic_by_broker = TopicByBroker.get(self.id, broker_id)
        if topic_by_broker is not None:
            topic_by_broker.delete()

    def add_subscriber(self, subscriber_id):
        topic_by_subscriber = TopicBySubscriber(
            topic_id=self.id, subscriber_id=subscriber_id)
        topic_by_subscriber.save()
        return topic_by_subscriber

    def remove_subscriber(self, subscriber_id):
        topic_by_subscriber = TopicBySubscriber.get(self.id, subscriber_id)
        if topic_by_subscriber is not None:
            topic_by_subscriber.delete()

    def _verify_consistency(self):
        if self.qos and self.qos not in (0, 1, 2,):
            raise ValueError("Invalid QoS level!")

    def save(self, *, refresh=False):
        """Write the data to the database.

        :param bool refresh: (optional, default False)
            Force to refresh this object data after commit.
        """
        self._verify_consistency()
        # This object was deleted and is detached from session.
        if self._db_state.was_deleted:
            # Set the object transient (session rollback of the deletion).
            sqla.orm.make_transient(self)
            for link in self.links:
                sqla.orm.make_transient(link)
        db.session.add(self)
        try:
            db.session.commit()
        except sqla.exc.IntegrityError as exc:
            db.session.rollback()
            raise exc
        if refresh:
            db.session.refresh(self)

    def delete(self):
        """Delete the item from the database."""
        # Verfify that object is not deleted yet to avoid a warning.
        if not self._db_state.was_deleted:
            db.session.delete(self)
            db.session.commit()

    def update_subscription(self, subscriber_id, is_subscribed):
        """Update topic subscription status.

        :param int subscriber_id:
            Unique subscriber ID that has subscribed/unsubscribe to topic.
        :param bool is_subscribed: Whether topic has been subscribed or not.
        """
        topic_by_subscriber = TopicBySubscriber.get(self.id, subscriber_id)
        if topic_by_subscriber is not None:
            topic_by_subscriber.update_subscription(is_subscribed)

    @classmethod
    def get_by_id(cls, id):
        """Find a subscriber by its ID stored in database.

        :param int id: Unique ID of the subscriber to find.
        :returns Subscriber: Instance found of `Subscriber`.
        """
        # Verfify that `id` exists to avoid a warning.
        if id is None:
            return None
        return db.session.get(cls, id)
