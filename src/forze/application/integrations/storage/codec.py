"""Path codec for object-storage object keys."""

from typing import Any, Final, final

import attrs

# ----------------------- #

_PATH_SEP: Final[str] = "/"


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ObjectStoragePathCodec:
    """Path codec for building namespaced object keys."""

    def join(self, *parts: Any) -> str:
        """Build a path from non-empty parts."""

        items = [str(p).strip(_PATH_SEP) for p in parts if p]

        return _PATH_SEP.join(items)

    # ....................... #

    def split(self, key: str) -> list[str]:
        """Split a path by the path separator."""

        return key.split(_PATH_SEP)

    # ....................... #

    def cond_join(self, *parts: Any | None) -> str:
        """Join only non-``None`` parts into a path."""

        items = list(filter(None, parts))

        return self.join(*items)


# ....................... #

default_path_codec = ObjectStoragePathCodec()
