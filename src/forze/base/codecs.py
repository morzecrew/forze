"""Encoding and decoding utilities for JSON, text, base64, and key/path handling.

Provides immutable codec classes for serialization, string encoding, and
namespace/key construction used across the application.
"""

import base64
from typing import Any

import attrs
import orjson

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class JsonCodec:
    """JSON serializer using orjson with deterministic key ordering.

    Uses ``OPT_SORT_KEYS`` for stable output. Supports both bytes and string
    input for :meth:`loads`.
    """

    encoding: str = "utf-8"
    """Character encoding for string conversion."""

    # ....................... #

    def dumps(self, value: Any) -> bytes:
        """Serialize a value to JSON bytes with sorted keys."""

        return orjson.dumps(value, option=orjson.OPT_SORT_KEYS)

    # ....................... #

    def loads(self, raw: bytes | str) -> Any:
        """Deserialize JSON from bytes or string. Strings are encoded with :attr:`encoding`."""

        if isinstance(raw, str):
            raw = raw.encode(self.encoding)

        return orjson.loads(raw)

    # ....................... #

    def dumps_as_str(self, value: Any) -> str:
        """Serialize to JSON and decode to string using :attr:`encoding`."""

        return self.dumps(value).decode(self.encoding)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TextCodec:
    """Plain text encoder/decoder for string-to-bytes conversion.

    Accepts both bytes and string input in :meth:`loads`; strings pass through
    unchanged.
    """

    encoding: str = "utf-8"
    """Character encoding for encode/decode operations."""

    # ....................... #

    def dumps(self, value: str) -> bytes:
        """Encode a string to bytes using :attr:`encoding`."""

        return value.encode(self.encoding)

    # ....................... #

    def loads(self, raw: bytes | str) -> str:
        """Decode bytes to string, or return string unchanged."""

        if isinstance(raw, str):
            return raw

        return raw.decode(self.encoding)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AsciiB64Codec:
    """Transparent base64 codec for non-ASCII strings.

    ASCII-only strings pass through unchanged. Non-ASCII strings are base64-encoded
    and prefixed with :attr:`prefix` for round-trip detection.
    """

    prefix: str = "b64://"
    """Prefix marking base64-encoded values in :meth:`loads`."""

    # ....................... #

    def dumps(self, value: str) -> str:
        """Return value as-is if ASCII, otherwise base64-encode with prefix."""

        if value.isascii():
            return value

        encoded = base64.b64encode(value.encode("utf-8")).decode("ascii")

        return f"{self.prefix}{encoded}"

    # ....................... #

    def loads(self, raw: str) -> str:
        """Decode base64 if prefixed, otherwise return raw string."""

        if raw.startswith(self.prefix):
            raw = raw[len(self.prefix) :]

            return base64.b64decode(raw.encode("ascii")).decode("utf-8")

        return raw
