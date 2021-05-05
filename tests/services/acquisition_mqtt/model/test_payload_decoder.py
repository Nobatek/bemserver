"""Payload decoder tests"""

import pytest
import sqlalchemy as sqla

from bemserver.services.acquisition_mqtt.model import (
    PayloadDecoder, PayloadField)

from tests.services.acquisition_mqtt.conftest import (
    PayloadDecoderMosquittoUptime)


class TestPayloadDecoderModel:

    def test_payload_decoder_crud(self, database):

        assert PayloadDecoder.get_by_id(1) is None
        assert PayloadDecoder.get_by_name(
            PayloadDecoderMosquittoUptime.name) is None

        decoder = PayloadDecoder(name=PayloadDecoderMosquittoUptime.name)
        assert decoder.id is None
        decoder.save()
        assert decoder.id is not None
        assert decoder.name == PayloadDecoderMosquittoUptime.name
        assert decoder.description is None
        assert decoder.topics == []
        assert decoder.fields == []

        assert PayloadDecoder.get_by_id(decoder.id) == decoder
        assert PayloadDecoder.get_by_name(decoder.name) == decoder

        payload_field = decoder.add_field("temperature")
        assert isinstance(payload_field, PayloadField)
        assert decoder.fields == [payload_field]

        other_fields = []
        for i in range(2):
            new_payload_field = decoder.add_field(f"test{i}")
            assert new_payload_field in decoder.fields
            other_fields.append(new_payload_field)
        assert len(decoder.fields) == len(other_fields) + 1

        decoder.remove_field(field_id=other_fields[0].id)
        assert decoder.fields == [payload_field, other_fields[1]]

        decoder.remove_field(field_name=other_fields[1].name)
        assert decoder.fields == [payload_field]

        # Deleting a payload decoder also removes payload fields in cascade.
        decoder.delete()
        assert PayloadField.get_by_id(payload_field.id) is None

    def test_payload_decoder_register(self, database):

        assert PayloadDecoder.get_by_name(
            PayloadDecoderMosquittoUptime.name) is None

        PayloadDecoder.register_from_class(PayloadDecoderMosquittoUptime)

        decoder = PayloadDecoder.get_by_name(
            PayloadDecoderMosquittoUptime.name)
        assert decoder is not None
        assert decoder.name == PayloadDecoderMosquittoUptime.name
        assert decoder.description == PayloadDecoderMosquittoUptime.description
        assert len(decoder.fields) == 1
        assert [x.name for x in decoder.fields] == (
            PayloadDecoderMosquittoUptime.fields)


class TestPayloadFieldModel:

    def test_payload_field_crud(self, database):

        decoder = PayloadDecoder(name=PayloadDecoderMosquittoUptime.name)
        decoder.save()

        assert PayloadField.get_by_id(1) is None
        assert PayloadField.get(decoder.id, "test") is None

        payload_field = PayloadField(
            payload_decoder_id=decoder.id, name="test")
        assert payload_field.id is None
        payload_field.save()
        assert payload_field.id is not None
        assert payload_field.payload_decoder_id == decoder.id
        assert payload_field.payload_decoder == decoder
        assert payload_field.name == "test"

        assert decoder.fields == [payload_field]

        assert PayloadField.get_by_id(payload_field.id) == payload_field
        assert PayloadField.get(
            decoder.id, payload_field.name) == payload_field

        # Unicity required on field names for each payload decoder.
        payload_field_bis = PayloadField(
            payload_decoder_id=decoder.id, name="test")
        with pytest.raises(sqla.exc.IntegrityError):
            payload_field_bis.save()

        decoder2 = PayloadDecoder(name="great_decoder")
        decoder2.save()
        payload_field2 = PayloadField(
            payload_decoder_id=decoder2.id, name="test")
        assert payload_field2 != payload_field

        payload_field.delete()
        assert PayloadField.get_by_id(1) is None
        assert PayloadField.get(decoder.id, "test") is None
