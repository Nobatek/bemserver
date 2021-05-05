"""MQTT subscriber"""

import time
import datetime as dt
import sqlalchemy as sqla
import paho.mqtt.client as mqttc
import paho.mqtt.properties as mqtt_props

from bemserver.core.database import Base, db
from bemserver.services.acquisition_mqtt.model import Broker


class Subscriber(Base):
    """The scubscriber describe how to connect to a broker.

    Subscriber client waits for messages as soon as it is connected.
    Subscriptions to topics must be done manually, calling `subscribe_all`
    (which automatically subscribes to all topics wanted by the subscriber) or
    `subscribe` (to a specific topic) methods. Same for unsubscriptions.

    :param bool is_enabled: (default True)
        To disable a subscriber client. Allows to not use it without deletion.
    :param int keep_alive: (default 60)
        Time interval, in seconds, to auto-check client connection status.
    :param bool use_persistent_session: (default True)
        If True makes client session persistent even after its disconnection.
        For this, subscriber's `client_id` is required when connecting.
        A persistent session keeps in memory which topics are subscribed and
        which messages have been received or not (also requires QoS>1).
    :param int session_expiry: (default 3600)
        Time, in seconds, to keep client session after its disconnection.
        (useless if session is not persistent)
    :param str description: (optional, default None)
        Text to describe the subscriber.
    :param str username: (optional, default None)
        Username that authenticates subscriber with the broker, if required.
    :param str password: (optional, default None)
        Password that authenticates subscriber with the broker, if required.
    :param int broker_id: Relation to a broker unique ID.
    :param bool is_connected: (optional, default False)
        Field auto-updated by `connect` and `disconnect` methods.
        This allows to known the last client connection status.
    :param datetime timestamp_last_connection: (optional, default None)
        Field auto-updated by `connect` and `disconnect` methods.
        This allows to known the last client connection timestamp.
    """
    __tablename__ = "mqtt_subscriber"

    id = sqla.Column(sqla.Integer, primary_key=True)
    is_enabled = sqla.Column(sqla.Boolean, nullable=False, default=True)
    keep_alive = sqla.Column(sqla.Integer, nullable=False, default=60)
    use_persistent_session = sqla.Column(
        sqla.Boolean, nullable=False, default=True)
    session_expiry = sqla.Column(sqla.Integer, nullable=False, default=3600)
    description = sqla.Column(sqla.String(250))
    username = sqla.Column(sqla.String)
    password = sqla.Column(sqla.String)
    broker_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey('mqtt_broker.id'),
        nullable=False,
    )
    is_connected = sqla.Column(sqla.Boolean, nullable=False, default=False)
    timestamp_last_connection = sqla.Column(sqla.DateTime(timezone=True))

    broker = sqla.orm.relationship("Broker", back_populates="subscribers")
    topics = sqla.orm.relationship("Topic", back_populates="subscriber")

    @property
    def must_authenticate(self):
        return self.broker.is_auth_required and self.username is not None

    @property
    def _db_state(self):
        return sqla.orm.util.object_state(self)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client_id = None
        self._client = None
        self._client_session_present = False

    def __repr__(self):
        str_fields = ", ".join([
            f"{x.name}={getattr(self, x.name)}" for x in self.__table__.columns
        ])
        return f"<{self.__table__.name}>({str_fields})"

    def _client_create(self):
        # Initialize paho MQTT client.
        client_kwargs = {
            "protocol": self.broker.protocol_version,
            "transport": self.broker.transport,
        }
        if self._client_id is not None:
            client_kwargs["client_id"] = self._client_id
        if self.broker.protocol_version in (mqttc.MQTTv31, mqttc.MQTTv311,):
            client_kwargs["clean_session"] = not self.use_persistent_session
        client = mqttc.Client(**client_kwargs)
        # Set client callbacks.
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_subscribe = self._on_subscribe
        client.on_unsubscribe = self._on_unsubscribe
        client.on_log = self._on_log
        return client

    def _client_apply_security(self):
        # Apply MQTT security (authentication, TLS...).
        if self.must_authenticate:
            self._client.username_pw_set(self.username, password=self.password)
        if self.broker.use_tls:
            self._client.tls_set(
                ca_certs=self.broker.tls_certificate_filepath,
                cert_reqs=self.broker.tls_verifymode,
                tls_version=self.broker.tls_version)

    def _client_connect(self):
        # Set client connection properties.
        cli_conn_kwargs = {
            "host": self.broker.host,
            "port": self.broker.port,
            "keepalive": self.keep_alive,
        }
        if self.broker.protocol_version == mqttc.MQTTv5:
            cli_conn_kwargs["clean_start"] = not self.use_persistent_session
            conn_props = mqtt_props.Properties(mqtt_props.PacketTypes.CONNECT)
            conn_props.SessionExpiryInterval = self.session_expiry
            cli_conn_kwargs["properties"] = conn_props
        # Connect the client.
        self._client.connect(**cli_conn_kwargs)
        # TODO: raise or log errors

    def connect(self, client_id=None):
        """Instantiate the MQTT client and connect it to its broker.

        :param str client_id: (optional, default None)
            Client ID to use, especially when using a persistent session.
        :raises ssl.SSLError: When TLS certificate is not valid.
        :raises ssl.SSLCertVerificationError: When TLS certificate expired.
        """
        self._client_id = client_id
        self._client = self._client_create()
        self._client_apply_security()
        self._client_connect()

        # It is important that subscriptions occurs before starting the
        #  waiting messages loop in order to receive stored messages for
        #  a persistent session.
        self.subscribe_all()
        # Run a threaded interface to the network loop in the background.
        #  (messages are received in this loop)
        self._client.loop_start()

        # TODO: Improve this in case to avoid infinite loop.
        # Wait for the connection to be effective.
        while (not self._client.is_connected()):
            time.sleep(0.1)
        self.is_connected = True
        self.timestamp_last_connection = dt.datetime.now(dt.timezone.utc)
        self.save()

    def _on_connect(
            self, client, userdata, flags, reasonCode, properties=None):
        reason_code = reasonCode
        if isinstance(reasonCode, mqttc.ReasonCodes):
            reason_code = reasonCode.value
        if reason_code != 0:
            # TODO: raise or log error
            # use mqttc.connack_string(reason_code) to get a clean string
            pass

        self._client_session_present = bool(flags.get("session present", 0))

        # TODO: publish message on subscriber client status topic (->online)?

    def disconnect(self):
        """Disconnect the MQTT client from its broker."""
        # At each disconnection, client subscriptions are lost event when
        #  persistent session is used. Persistent session just means that
        #  broker will keep messages for the subscriber when it reconnects
        #  and re-subscribes to those topics.
        # Update topics' subscription states in database.
        for topic in self.topics:
            topic.update_subscription(False)

        self._client.disconnect()

        # TODO: Improve this in case to avoid infinite loop.
        # Wait for the disconnection to be effective.
        while (self._client.is_connected()):
            time.sleep(0.1)
        self.is_connected = False
        self.save()

        # Kill the network loop that receives messages.
        self._client.loop_stop()

    def _on_disconnect(self, client, userdata, reasonCode, properties=None):
        if reasonCode != 0:
            # TODO: raise or log error
            # use mqttc.connack_string(reasonCode) to get a clean string
            pass

        # TODO: publish message on subscriber client status topic (->offline)?

    def subscribe(self, topic):
        """Make the MQTT client subscribe to the defined topic.

        :param Topic topic: Topic instance to subscribe to.
        """
        self._client.message_callback_add(
            topic.name, topic.payload_decoder_instance.on_message)
        self._client.subscribe(topic.name, topic.qos)
        topic.update_subscription(True)

    def subscribe_all(self):
        """Automatically make the MQTT client subscribe to all its topics."""
        for topic in self.topics:
            self.subscribe(topic)

    def _on_subscribe(
            self, client, userdata, mid, granted_qos, properties=None):
        pass

    def unsubscribe(self, topic):
        """Make the MQTT client unsubscribe from the defined topic.

        :param Topic topic: Topic instance to unsubscribe from.
        """
        self._client.unsubscribe(topic.name)
        topic.update_subscription(False)

    def unsubscribe_all(self):
        """Automatically make the MQTT client unsubscribe from all its topics.
        """
        for topic in self.topics:
            self.unsubscribe(topic)

    def _on_unsubscribe(self, client, userdata, mid, properties, reasonCode):
        pass

    def enable_logger(self, logger=None):
        self._client.enable_logger(logger=logger)

    def disable_logger(self):
        self._client.disable_logger()

    def _on_log(self, client, userdata, level, buf):
        pass

    def _verify_consistency(self):
        broker = Broker.get_by_id(self.broker_id)
        if broker.is_auth_required:
            if self.username is None:
                raise ValueError("Broker requires username authentication!")
        elif self.username is not None:
            # TODO: warn that subscriber authentication data is useless
            pass

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
            try:
                db.session.commit()
            except sqla.exc.IntegrityError as exc:
                db.session.rollback()
                raise exc

    @classmethod
    def get_by_id(cls, id):
        """Find a subscriber by its ID stored in database.

        :param int id: Unique ID of the subscriber to find.
        :returns Subscriber: Instance of subscriber found.
        """
        # Verfify that `id` exists to avoid a warning.
        if id is None:
            return None
        try:
            return db.session.get(cls, id)
        except sqla.orm.exc.ObjectDeletedError:
            return None

    @classmethod
    def get_list(cls, is_enabled=None):
        """List all subscribers stored in database.

        :param bool is_enabled: (optional, default None)
            Filter subscribers on `is_enabled` field value. Ignored if `None`.
        :returns list: The list of subscribers found.
        """
        stmt = sqla.select(cls)
        if is_enabled is not None:
            stmt = stmt.filter(cls.is_enabled == is_enabled)
        stmt = stmt.order_by(cls.id)
        return db.session.execute(stmt).all()