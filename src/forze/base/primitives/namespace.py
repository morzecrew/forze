from functools import lru_cache

import attrs

from ..exceptions import exc
from .types import StrKey

# ----------------------- #


def _validate_parts(sep: str, *parts: StrKey) -> None:
    for p in parts:
        x = str(p)

        if not x:
            raise exc.internal("Key part must be non-empty")

        if sep in x:
            raise exc.internal(f"Key part must not contain separator '{sep}'")


# ....................... #


@lru_cache(maxsize=1024)
def _joined_key(sep: str, *parts: str) -> str:
    """Validate and join the already rendered *parts* with *sep*.

    Memoized because an operation facade rebuilds the same key on every call. The caller
    passes each part's ``str()``, which is all the validation and the join read, so parts
    that render differently never share an entry: a mixed-in ``(str, Enum)`` member equals
    its value yet renders as ``Cls.NAME``, and a ``str`` subclass may render as it likes.
    An invalid part raises, and a raise is never cached. Bounded (``maxsize=1024``) since
    parts are caller-controlled; an evicted entry is simply rebuilt.
    """

    _validate_parts(sep, *parts)

    return sep.join(parts)


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class StrKeyNamespace:
    """Stable namespace for string keys."""

    prefix: StrKey
    """Prefix for the namespace."""

    sep: str = "."
    """Separator for the namespace key parts."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _validate_parts(self.sep, self.prefix)

    # ....................... #

    def key(self, *parts: StrKey) -> str:
        if not parts:
            raise exc.internal("No parts provided")

        return _joined_key(self.sep, str(self.prefix), *map(str, parts))
