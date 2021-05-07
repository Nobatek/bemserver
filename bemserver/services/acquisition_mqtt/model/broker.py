"""MQTT broker"""

import enum
import ssl
import logging
import sqlalchemy as sqla
import paho.mqtt.client as mqttc
from pathlib import Path

from bemserver.core.database import Base, db
from bemserver.services.acquisition_mqtt import SERVICE_LOGNAME


logger = logging.getLogger(SERVICE_LOGNAME)


class Broker(Base):
    """Describes each broker configuration.

    :param str host: Host name used for the connection to the broker.
    :param int port: (default 1883)
        Host port used for the connection to the broker.
    :param int protocol_version: (default 5)
        MQTT protocol version required by the broker.
    :param str transport: (default "tcp")
        Transport mode used for the connection to the broker.
    :param str description: (optional, default None)
        Text to describe the broker.
    :param bool is_auth_required: (default False)
        Does connection to the broker requires authentication (username...)?
    :param bool use_tls: (default False)
        Does connection need to be encrypted (by using TLS)?
    :param int tls_version: (default ssl.PROTOCOL_TLSv1_2)
        The version number of TLS used for the encrypted connection.
    :param int tls_verifymode: (default ssl.CERT_OPTIONAL)
        Is the TLS certificate required or optional?
    :param str tls_certificate: The content of certificate file.
    """
    __tablename__ = "mqtt_broker"

    class Transport(enum.Enum):
        tcp = "tcp"
        websockets = "websockets"

    id = sqla.Column(sqla.Integer, primary_key=True)
    host = sqla.Column(sqla.String(250), nullable=False)
    port = sqla.Column(sqla.Integer, nullable=False, default=1883)
    protocol_version = sqla.Column(
        sqla.Integer, nullable=False, default=mqttc.MQTTv5)
    transport = sqla.Column(
        sqla.String, nullable=False, default=Transport.tcp.value)
    description = sqla.Column(sqla.String(250))
    is_auth_required = sqla.Column(sqla.Boolean, nullable=False, default=False)
    use_tls = sqla.Column(sqla.Boolean, default=False)
    tls_version = sqla.Column(
        sqla.Integer, nullable=False, default=ssl.PROTOCOL_TLSv1_2)
    tls_verifymode = sqla.Column(
        sqla.Integer, nullable=False, default=ssl.CERT_OPTIONAL)
    tls_certificate = sqla.Column(sqla.String)

    subscribers = sqla.orm.relationship("Subscriber", back_populates="broker")
    topics = sqla.orm.relationship(
        "Topic", secondary="mqtt_topic_by_broker", back_populates="brokers")

    @property
    def tls_certificate_filepath(self):
        if self.tls_certificate_dirpath is not None:
            return self.tls_certificate_dirpath / self._tls_cert_filename
        return None

    @property
    def tls_certificate_dirpath(self):
        if self._tls_cert_dirpath is not None:
            return Path(self._tls_cert_dirpath)
        return None

    @tls_certificate_dirpath.setter
    def tls_certificate_dirpath(self, value):
        self._tls_cert_dirpath = Path(value)
        self._generate_tls_cert_file()

    @property
    def _db_state(self):
        return sqla.orm.util.object_state(self)

    @property
    def _log_header(self):
        return f"[Broker #{self.id} @{self.host}]"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tls_cert_dirpath = None
        self._tls_cert_filename = f"{self.host}.crt"

    def __repr__(self):
        str_fields = ", ".join([
            f"{x.name}={getattr(self, x.name)}" for x in self.__table__.columns
        ])
        return f"<{self.__table__.name}>({str_fields})"

    def _verify_consistency(self):
        if self.protocol_version and self.protocol_version not in (
                mqttc.MQTTv31, mqttc.MQTTv311, mqttc.MQTTv5,):
            raise ValueError("Invalid broker protocol version!")

        if self.transport and self.transport not in tuple(
                x.value for x in Broker.Transport):
            raise ValueError("Invalid broker transport mode!")

        if self.use_tls:
            if self.tls_certificate is None:
                raise ValueError("Missing certificate data!")
            if self.tls_verifymode == ssl.CERT_NONE:
                logger.warning(
                    f"{self._log_header} TLS verify mode (CERT_NONE) not"
                    " recommended as untrusted or expired cert are ignored and"
                    " do not abort the TLS/SSL handshake.")

            if self.tls_version and self.tls_version not in (
                    ssl.PROTOCOL_TLSv1, ssl.PROTOCOL_TLSv1_1,
                    ssl.PROTOCOL_TLSv1_2,):
                raise ValueError("Invalid broker TLS version!")

            if self.tls_verifymode and self.tls_verifymode not in tuple(
                    x.value for x in ssl.VerifyMode):
                raise ValueError("Invalid broker TLS verify mode!")

    def _generate_tls_cert_file(self):
        logger.info(f"{self._log_header} generating TLS cert file...")
        with open(str(self.tls_certificate_filepath), "w") as f:
            f.write(self.tls_certificate)
        logger.debug(f"{self._log_header} TLS cert file generated!")

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
        """Find a broker by its ID stored in database.

        :param int id: Unique ID of the broker to find.
        :returns Subscriber: Instance of broker found.
        """
        # Verfify that `id` exists to avoid a warning.
        if id is None:
            return None
        return db.session.get(cls, id)
