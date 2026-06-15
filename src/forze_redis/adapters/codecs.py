from datetime import datetime
from typing import Any, Final, Mapping, final

import attrs
import orjson

from forze.application.contracts.crypto import (
    is_encrypted_payload,
    looks_encrypted_body,
)
from forze.application.contracts.pubsub import PubSubMessage
from forze.application.contracts.stream import StreamMessage
from forze.base.codecs import JsonCodec, TextCodec
from forze.base.exceptions import exc
from forze.base.serialization import ModelCodec

# ----------------------- #

default_json_codec = JsonCodec()
default_text_codec = TextCodec()

KEY_SEP: Final[str] = ":"

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisKeyCodec:
    """Redis key codec for building namespaced keys."""

    namespace: str
    """Namespace for Redis keys."""

    sep: str = KEY_SEP
    """Separator between key parts."""

    # ....................... #

    def join(
        self,
        prefix: str | tuple[str, ...] | None,
        scope: str | tuple[str, ...],
        *parts: Any,
    ) -> str:
        if isinstance(prefix, tuple):
            prefix = self.sep.join(prefix)

        if isinstance(scope, tuple):
            scope = self.sep.join(scope)

        items = [
            str(p).strip(self.sep) for p in (prefix, scope, self.namespace, *parts) if p
        ]

        return self.sep.join(items)


# ....................... #

_F_PAYLOAD: Final[str] = "payload"
_F_TYPE: Final[str] = "type"
_F_PUBLISHED_AT: Final[str] = "published_at"
_F_KEY: Final[str] = "key"
_F_TIMESTAMP: Final[str] = "timestamp"
_F_HEADERS: Final[str] = "headers"

# ....................... #


def _encode_envelope[M](
    payload_codec: ModelCodec[M, Any],
    payload: M,
    *,
    type: str | None,
    key: str | None,
    time_field: str,
    time_value: datetime | None,
    headers: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Build the field envelope shared by the pubsub and stream encoders.

    *headers* ride the JSON envelope as a JSON-encoded object under the
    ``headers`` field, next to ``type``/``key`` — caller keys pass through
    verbatim (the envelope field names cannot collide with header keys
    because headers live in their own nested object).
    """

    # An end-to-end encrypted payload arrives as the one-key envelope wrapper (the relay
    # forwards ciphertext). Serialize it opaquely into the payload field — the model codec
    # would reject a wrapper that is not its own model. The consumer decrypts it.
    payload_field = (
        orjson.dumps(payload).decode()
        if is_encrypted_payload(payload)
        else payload_codec.encode_json_bytes(payload).decode()
    )

    data: dict[str, str] = {_F_PAYLOAD: payload_field}

    if type is not None:
        data[_F_TYPE] = type

    if key is not None:
        data[_F_KEY] = key

    if time_value is not None:
        data[time_field] = time_value.isoformat()

    if headers:
        data[_F_HEADERS] = default_json_codec.dumps(dict(headers)).decode()

    return data


# ....................... #


def _decode_payload_field[M](payload_codec: ModelCodec[M, Any], payload_raw: str) -> M:
    """Decode the payload envelope field, passing an encrypted wrapper through.

    A one-key envelope wrapper (end-to-end ciphertext) survives to the consumer as the
    message payload, for it to decrypt; a plaintext field decodes to the model as before.
    """

    if looks_encrypted_body(payload_raw.encode("utf-8")):
        return orjson.loads(payload_raw)  # type: ignore[no-any-return]

    return payload_codec.decode_json_bytes(payload_raw)


# ....................... #


def _decode_headers(raw: object) -> dict[str, str]:
    """Decode the JSON-encoded ``headers`` envelope field (best-effort)."""

    if not isinstance(raw, str):
        return {}

    try:
        decoded = default_json_codec.loads(raw)
    except Exception:
        return {}

    if not isinstance(decoded, dict):
        return {}

    out: dict[str, str] = {}

    for k, v in decoded.items():  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if isinstance(k, str) and isinstance(v, str):
            out[k] = v

    return out


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisPubSubCodec[M]:
    """JSON codec that serialises and deserialises :class:`~forze.application.contracts.pubsub.PubSubMessage` payloads.

    :meth:`encode` wraps a payload record into a JSON envelope with optional
    metadata fields (``type``, ``key``, ``published_at``).  :meth:`decode`
    reconstructs the :class:`~forze.application.contracts.pubsub.PubSubMessage`
    from the raw channel bytes.
    """

    payload_codec: ModelCodec[M, Any]
    """Codec for pubsub message payloads."""

    # ....................... #

    def encode(
        self,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        published_at: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> bytes:
        data = _encode_envelope(
            self.payload_codec,
            payload,
            type=type,
            key=key,
            time_field=_F_PUBLISHED_AT,
            time_value=published_at,
            headers=headers,
        )

        return default_json_codec.dumps(data)

    # ....................... #

    def decode(self, topic: str, raw_data: bytes | str) -> PubSubMessage[M]:
        decoded = default_json_codec.loads(raw_data)

        if not isinstance(decoded, dict):
            raise exc.internal(f"Redis pubsub message in '{topic}' has invalid payload")

        payload_raw = decoded.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            _F_PAYLOAD
        )

        if not isinstance(payload_raw, str):
            raise exc.internal(f"Redis pubsub message in '{topic}' has no payload")

        type_raw = decoded.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            _F_TYPE
        )
        key_raw = decoded.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            _F_KEY
        )
        published_at_raw = decoded.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            _F_PUBLISHED_AT
        )
        headers_raw = decoded.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            _F_HEADERS
        )

        return PubSubMessage(
            topic=topic,
            payload=_decode_payload_field(self.payload_codec, payload_raw),
            type=type_raw if isinstance(type_raw, str) else None,
            key=key_raw if isinstance(key_raw, str) else None,
            published_at=(
                datetime.fromisoformat(published_at_raw)
                if isinstance(published_at_raw, str)
                else None
            ),
            headers=_decode_headers(headers_raw),  # pyright: ignore[reportUnknownArgumentType]
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamCodec[M]:
    """JSON codec for Redis Stream entries.

    :meth:`encode` converts a payload record into a ``dict[str, str]`` suitable
    for ``XADD``.  :meth:`decode` reconstructs a
    :class:`~forze.application.contracts.stream.StreamMessage` from the raw
    bytes-keyed field map returned by ``XREAD`` / ``XREADGROUP``.
    """

    payload_codec: ModelCodec[M, Any]
    """Codec for stream message payloads."""

    # ....................... #

    def encode(
        self,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        timestamp: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        return _encode_envelope(
            self.payload_codec,
            payload,
            type=type,
            key=key,
            time_field=_F_TIMESTAMP,
            time_value=timestamp,
            headers=headers,
        )

    # ....................... #

    def decode(
        self,
        stream: str,
        id: str,
        raw_data: dict[bytes, bytes],
    ) -> StreamMessage[M]:
        decoded = {k.decode("utf-8"): v.decode("utf-8") for k, v in raw_data.items()}
        payload_raw = decoded.get(_F_PAYLOAD)

        if payload_raw is None:
            raise exc.internal(
                f"Redis stream message '{id}' in '{stream}' has no payload"
            )

        timestamp_raw = decoded.get(_F_TIMESTAMP)

        return StreamMessage(
            stream=stream,
            id=id,
            payload=_decode_payload_field(self.payload_codec, payload_raw),
            type=decoded.get(_F_TYPE),
            key=decoded.get(_F_KEY),
            timestamp=datetime.fromisoformat(timestamp_raw) if timestamp_raw else None,
            headers=_decode_headers(decoded.get(_F_HEADERS)),
        )
