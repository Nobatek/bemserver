"""MQTT topic"""

import datetime as dt
import sqlalchemy as sqla

from bemserver.core.database import Base, db
from bemserver.services.acquisition_mqtt import decoders


class Topic(Base):
    """Describes how topics should be subscribed and how payload is decoded.

    :param str name: Topic name (for example: "sensors/temperature/1").
    :param int qos: (default 1)
        Level of Quality of Service (QoS) used for this topic.
    :param str description: (optional, default None)
        Text to describe the topic.
    :param str payload_decoder: (default "bemserver")
        Name of the code used to decode the payload from received messages.
    :param bool is_enabled: (optional, default True)
    :param bool is_subscribed: (optional, default False)
        This allows to known the last topic subscription status.
    :param datetime timestamp_last_subscription: (optional, default None)
        Field auto-updated by `update_subscription` method.
        This allows to known the last topic subscription timestamp.
    :param int timeseries_id: Relation to a timeseries unique ID.
    :param subscriber_id: Relation to a subscriber unique ID.
    """
    __tablename__ = "mqtt_topic"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(250), unique=True, nullable=False)
    qos = sqla.Column(sqla.Integer, nullable=False, default=1)
    description = sqla.Column(sqla.String(250))
    payload_decoder = sqla.Column(
        sqla.String,
        nullable=False,
        default=decoders.PayloadDecoderBEMServer.name,
    )
    is_enabled = sqla.Column(sqla.Boolean, nullable=False, default=True)
    is_subscribed = sqla.Column(sqla.Boolean, nullable=False, default=False)
    timestamp_last_subscription = sqla.Column(sqla.DateTime(timezone=True))
    timeseries_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("timeseries.id"),
        nullable=False,
    )
    subscriber_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("mqtt_subscriber.id"),
        nullable=False,
    )

    timeseries = sqla.orm.relationship("Timeseries", backref=__tablename__)
    subscriber = sqla.orm.relationship("Subscriber", back_populates="topics")

    @property
    def payload_decoder_cls(self):
        return decoders.get_payload_decoder(self.payload_decoder)

    @property
    def payload_decoder_instance(self):
        return self.payload_decoder_cls(self)

    @property
    def _db_state(self):
        return sqla.orm.util.object_state(self)

    def __repr__(self):
        str_fields = ", ".join([
            f"{x.name}={getattr(self, x.name)}" for x in self.__table__.columns
        ])
        return f"<{self.__table__.name}>({str_fields})"

    def _verify_consistency(self):
        if self.qos and self.qos not in (0, 1, 2,):
            raise ValueError("Invalid QoS level!")
        if self.payload_decoder and not decoders.is_payload_decoder_registered(
                self.payload_decoder):
            raise ValueError(
                f"{self.payload_decoder} payload decoder is not registered!")

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
        db.session.add(self)
        db.session.commit()
        if refresh:
            db.session.refresh(self)

    def delete(self):
        """Delete the item from the database."""
        # Verfify that object is not deleted yet to avoid a warning.
        if not self._db_state.was_deleted:
            db.session.delete(self)
            db.session.commit()

    def update_subscription(self, is_subscribed):
        """Update topic subscription status.

        :param bool is_subscribed: Whether topic has been subscribed or not.
        """
        self.is_subscribed = is_subscribed
        if is_subscribed:
            # Only update this field value at subscription time.
            self.timestamp_last_subscription = dt.datetime.now(dt.timezone.utc)
        self.save()

    @classmethod
    def get_by_id(cls, id):
        """Find a subscriber by its ID stored in database.

        :param int id: Unique ID of the subscriber to find.
        :returns Subscriber: Instance of subscriber found.
        """
        # Verfify that `id` exists to avoid a warning.
        if id is None:
            return None
        return db.session.get(cls, id)

    @classmethod
    def get_list(cls, subscriber_id, is_enabled=None):
        """List all topics stored in database.

        :param int subscriber_id: Filter topics on `subscriber_id` field value.
            Ignored if ̀ None`.
        :param bool is_enabled: (optional, default None)
            Filter topics on `is_enabled` field value. Ignored if `None`.
        :returns list: The list of topics found.
        """
        stmt = sqla.select(cls)
        if subscriber_id is not None:
            stmt = stmt.filter(cls.subscriber_id == subscriber_id)
        if is_enabled is not None:
            stmt = stmt.filter(cls.is_enabled == is_enabled)
        stmt = stmt.order_by(cls.id)
        return db.session.execute(stmt).all()
