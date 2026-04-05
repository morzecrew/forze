from datetime import datetime
from typing import Any, Final, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.pubsub import PubSubMessage
from forze.application.contracts.stream import StreamMessage
from forze.base.codecs import JsonCodec, TextCodec
from forze.base.errors import CoreError

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

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisPubSubCodec[M: BaseModel]:
    """JSON codec that serialises and deserialises :class:`~forze.application.contracts.pubsub.PubSubMessage` payloads.

    :meth:`encode` wraps a Pydantic model into a JSON envelope with optional
    metadata fields (``type``, ``key``, ``published_at``).  :meth:`decode`
    reconstructs the :class:`~forze.application.contracts.pubsub.PubSubMessage`
    from the raw channel bytes.
    """

    model: type[M]
    """Pydantic model type."""

    # ....................... #

    def encode(
        self,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        published_at: datetime | None = None,
    ) -> bytes:
        data: dict[str, str] = {_F_PAYLOAD: payload.model_dump_json()}

        if type is not None:
            data[_F_TYPE] = type

        if key is not None:
            data[_F_KEY] = key

        if published_at is not None:
            data[_F_PUBLISHED_AT] = published_at.isoformat()

        return default_json_codec.dumps(data)

    # ....................... #

    def decode(self, topic: str, raw_data: bytes | str) -> PubSubMessage[M]:
        decoded = default_json_codec.loads(raw_data)

        if not isinstance(decoded, dict):
            raise CoreError(f"Redis pubsub message in '{topic}' has invalid payload")

        payload_raw = decoded.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            _F_PAYLOAD
        )

        if not isinstance(payload_raw, str):
            raise CoreError(f"Redis pubsub message in '{topic}' has no payload")

        type_raw = decoded.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            _F_TYPE
        )
        key_raw = decoded.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            _F_KEY
        )
        published_at_raw = decoded.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            _F_PUBLISHED_AT
        )

        return PubSubMessage(
            topic=topic,
            payload=self.model.model_validate_json(payload_raw),
            type=type_raw if isinstance(type_raw, str) else None,
            key=key_raw if isinstance(key_raw, str) else None,
            published_at=(
                datetime.fromisoformat(published_at_raw)
                if isinstance(published_at_raw, str)
                else None
            ),
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamCodec[M: BaseModel]:
    """JSON codec for Redis Stream entries.

    :meth:`encode` converts a Pydantic model into a ``dict[str, str]`` suitable
    for ``XADD``.  :meth:`decode` reconstructs a
    :class:`~forze.application.contracts.stream.StreamMessage` from the raw
    bytes-keyed field map returned by ``XREAD`` / ``XREADGROUP``.
    """

    model: type[M]
    """Pydantic model type."""

    # ....................... #

    def encode(
        self,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        timestamp: datetime | None = None,
    ) -> dict[str, str]:
        data: dict[str, str] = {_F_PAYLOAD: payload.model_dump_json()}

        if type is not None:
            data[_F_TYPE] = type

        if key is not None:
            data[_F_KEY] = key

        if timestamp is not None:
            data[_F_TIMESTAMP] = timestamp.isoformat()

        return data

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
            raise CoreError(f"Redis stream message '{id}' in '{stream}' has no payload")

        timestamp_raw = decoded.get(_F_TIMESTAMP)

        return StreamMessage(
            stream=stream,
            id=id,
            payload=self.model.model_validate_json(payload_raw),
            type=decoded.get(_F_TYPE),
            key=decoded.get(_F_KEY),
            timestamp=datetime.fromisoformat(timestamp_raw) if timestamp_raw else None,
        )
