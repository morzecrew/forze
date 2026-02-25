import base64
from typing import Any, Optional

import attrs
import orjson

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class JsonCodec:
    encoding: str = "utf-8"

    # ....................... #

    def dumps(self, value: Any) -> bytes:
        return orjson.dumps(value, option=orjson.OPT_SORT_KEYS)

    # ....................... #

    def loads(self, raw: bytes | str) -> Any:
        if isinstance(raw, str):
            raw = raw.encode(self.encoding)

        return orjson.loads(raw)

    # ....................... #

    def dumps_as_str(self, value: Any) -> str:
        return self.dumps(value).decode(self.encoding)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TextCodec:
    encoding: str = "utf-8"

    # ....................... #

    def dumps(self, value: str) -> bytes:
        return value.encode(self.encoding)

    # ....................... #

    def loads(self, raw: bytes | str) -> str:
        if isinstance(raw, str):
            return raw

        return raw.decode(self.encoding)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AsciiB64Codec:
    prefix: str = "b64://"

    # ....................... #

    def dumps(self, value: str) -> str:
        try:
            value.encode("ascii")
            return value

        except UnicodeEncodeError:
            encoded = base64.b64encode(value.encode("utf-8")).decode("ascii")
            return f"{self.prefix}{encoded}"

    # ....................... #

    def loads(self, raw: str) -> str:
        if raw.startswith(self.prefix):
            raw = raw[len(self.prefix) :]

            return base64.b64decode(raw.encode("ascii")).decode("utf-8")

        return raw


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class KeyCodec:
    namespace: str
    sep: str = ":"

    # ....................... #

    def join(self, *parts: str) -> str:
        items = [p.strip(self.sep) for p in (self.namespace, *parts) if p]

        return self.sep.join(items)

    # ....................... #

    def split(self, key: str) -> list[str]:
        return key.split(self.sep)

    # ....................... #

    def cond_join(self, *parts: Optional[str]) -> str:
        items = list(filter(None, parts))

        return self.join(*items)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PathCodec(KeyCodec):
    sep: str = attrs.field(default="/", init=False)
