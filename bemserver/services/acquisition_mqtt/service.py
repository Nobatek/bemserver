"""MQTT service"""

from pathlib import Path

from bemserver.core.database import db
from bemserver.services.acquisition_mqtt import decoders
from bemserver.services.acquisition_mqtt.model import (
    Subscriber, PayloadDecoder)
from bemserver.services.acquisition_mqtt.exceptions import ServiceError


# TODO: logging


MQTT_CLIENT_ID = "bemserver-acquisition"


class Service:

    def __init__(self, working_dirpath):
        self._tls_cert_dirpath = Path(working_dirpath)
        self._running_subscribers = []
        self.is_running = False

    def set_db_url(self, db_url):
        """Set database URL."""
        if (db.engine is None
                or db.engine is not None and str(db.engine.url) != db_url):
            db.set_db_url(db_url)

    def _register_decoders(self):
        for decoder_cls in decoders._PAYLOAD_DECODERS.values():
            PayloadDecoder.register_from_class(decoder_cls)

    def run(self, *, client_id=MQTT_CLIENT_ID):
        """Run the MQTT acquisition servive:
            - register payload decoders
            - get all enabled subsribers
            - connect each subscriber to its broker to get messages

        :param str client_id: (optional, default "bemserver-acquisition")
            Client ID to use, especially when using a persistent session.
        :raises ServiceError: When no enabled subscriber is available.
        """
        self._register_decoders()

        rows = Subscriber.get_list(is_enabled=True)
        if len(rows) <= 0:
            raise ServiceError(
                "No subscribers available to run MQTT acquisition!")

        for row in rows:
            subscriber = row[0]
            # Set certificate file path if broker uses TLS.
            if subscriber.broker.use_tls:
                subscriber.broker.tls_certificate_dirpath = (
                    self._tls_cert_dirpath)
            # Connect subscriber.
            subscriber.connect(client_id)
            if subscriber.is_connected:
                self._running_subscribers.append(subscriber)

        self.is_running = True

    def stop(self):
        """Stop the MQTT acquisition service.

        Each enabled subscriber is disconnected.
        """
        while len(self._running_subscribers) > 0:
            self._running_subscribers[0].disconnect()
            if not self._running_subscribers[0].is_connected:
                del self._running_subscribers[0]
        self.is_running = False
