"""Decoder for Chirpstack payloads"""

# import json
# import datetime as dt

from .base import PayloadDecoderBase


class PayloadDecoderChirpstack(PayloadDecoderBase):

    name = "chirpstack"

    def _decode(self, raw_payload):
        # json_payload = json.loads(raw_payload)
        # # example: 2021-04-16T14:03:13.432986Z
        # timestamp = dt.datetime.fromisoformat(
        #     json_payload["rxInfo"][0]["time"].replace("Z", "+00:00"))
        # obj_data = json.loads(json_payload["objectJSON"])
        # print(f"data: {obj_data}")
        # return timestamp, 0
        raise NotImplementedError
