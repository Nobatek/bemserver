"""Service exceptions"""


class ServiceError(Exception):
    """Service error."""


class PayloadDecoderError(Exception):
    """Payload decoder error."""


class PayloadDecoderNotFoundError(PayloadDecoderError):
    """Payload decoder class does not exist."""
