"""Kafka record codec: payload ⇄ value bytes, message metadata ⇄ native headers.

Unlike the Redis stream codec — which JSON-wraps ``type``/``key``/``headers`` into
one envelope field because Redis stream entries are a flat field map — Kafka has
native record headers, a native key, and a native timestamp, so this codec maps
onto them directly: the payload is the record **value**, the message ``type`` is a
reserved ``forze_type`` header, and transport headers ride the record's native
header list. An end-to-end-encrypted payload survives to the consumer opaquely
(the relay forwards ciphertext), exactly as the Redis codec handles it.
"""

from typing import Any, Final, Mapping, Sequence, final

import attrs
import orjson

from forze.application.contracts.crypto import (
    is_encrypted_payload,
    looks_encrypted_body,
)
from forze.base.serialization import ModelCodec

# ----------------------- #

_HEADER_TYPE: Final[str] = "forze_type"
"""Reserved header carrying the message ``type``; pulled back out on decode."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class KafkaStreamCodec[M]:
    """Encode/decode a :class:`StreamMessage` payload against Kafka's record shape."""

    payload_codec: ModelCodec[M, Any]
    """Codec for stream message payloads."""

    # ....................... #

    def encode_value(self, payload: M) -> bytes:
        """Serialize *payload* to the record value bytes.

        An end-to-end ciphertext wrapper (a one-key envelope, produced by the
        encrypting stream command) is serialized opaquely — the model codec would
        reject a wrapper that is not its own model; the consumer decrypts it.
        """

        if is_encrypted_payload(payload):
            return orjson.dumps(payload)

        return self.payload_codec.encode_json_bytes(payload)

    # ....................... #

    def decode_value(self, raw: bytes) -> M:
        """Decode the record value, passing an encrypted wrapper through."""

        if looks_encrypted_body(raw):
            candidate = orjson.loads(raw)

            if is_encrypted_payload(candidate):
                return candidate  # type: ignore[no-any-return]

        return self.payload_codec.decode_json_bytes(raw)

    # ....................... #

    def encode_headers(
        self,
        *,
        type: str | None,
        headers: Mapping[str, str] | None,
    ) -> list[tuple[str, bytes]]:
        """Build the native Kafka header list from transport *headers* + ``type``.

        Caller / envelope headers (``forze_event_id``, ``traceparent``, …) pass
        through verbatim; the reserved ``forze_type`` header carries the message
        type and wins over any caller key of that name.
        """

        out: list[tuple[str, bytes]] = []

        if headers:
            out.extend(
                (k, v.encode("utf-8")) for k, v in headers.items() if k != _HEADER_TYPE
            )

        if type is not None:
            out.append((_HEADER_TYPE, type.encode("utf-8")))

        return out

    # ....................... #

    def decode_headers(
        self,
        raw: Sequence[tuple[str, bytes]] | None,
    ) -> tuple[dict[str, str], str | None]:
        """Split native record headers into ``(transport headers, message type)``.

        ``forze_type`` is lifted into the returned ``type``; every other header is
        surfaced as ``StreamMessage.headers`` so the inbox path reads
        ``forze_event_id`` / ``traceparent`` / correlation exactly as produced.
        """

        headers: dict[str, str] = {}
        message_type: str | None = None

        for key, value in raw or ():
            decoded = value.decode("utf-8") if isinstance(value, bytes) else str(value)

            if key == _HEADER_TYPE:
                message_type = decoded
            else:
                headers[key] = decoded

        return headers, message_type
