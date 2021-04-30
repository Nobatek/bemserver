"""MQTT payload decoders"""

from bemserver.services.acquisition_mqtt.exceptions import (
    PayloadDecoderError, PayloadDecoderNotFoundError)

from .base import PayloadDecoderBase
from .bemserver import PayloadDecoderBEMServer
# from .chirpstack import PayloadDecoderChirpstack


_PAYLOAD_DECODERS = {
    PayloadDecoderBEMServer.name: PayloadDecoderBEMServer,
    # PayloadDecoderChirpstack.name: PayloadDecoderChirpstack,
}


def register_payload_decoder(payload_decoder_cls):
    """Register a new payload decoder class.

    :param PayloadDecoderBase payload_decoder_cls: Payload decoder class.
    :raises PayloadDecoderError: When payload decoder class is not valid.
    """
    if not issubclass(payload_decoder_cls, PayloadDecoderBase):
        raise PayloadDecoderError(
            f"{payload_decoder_cls} does not subclass {PayloadDecoderBase}!")
    _PAYLOAD_DECODERS[payload_decoder_cls.name] = payload_decoder_cls


def is_payload_decoder_registered(payload_decoder_name):
    return payload_decoder_name in _PAYLOAD_DECODERS


def get_payload_decoder(payload_decoder_name):
    """Get a registered payload decoder class from its name.

    :param str payload_decoder_name:
    :returns PayloadDecoderBase: Payload decoder class found.
    :raises PayloadDecoderNotFoundError: When payload decoder does not exist.
    """
    try:
        return _PAYLOAD_DECODERS[payload_decoder_name]
    except KeyError:
        raise PayloadDecoderNotFoundError(
            f"{payload_decoder_name} decoder not found!")
