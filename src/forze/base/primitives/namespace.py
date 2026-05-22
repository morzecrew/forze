import attrs

from ..errors import CoreError
from .types import StrKey

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class StrKeyNamespace:
    """Stable namespace for string keys."""

    prefix: StrKey
    """Prefix for the namespace."""

    sep: str = "."
    """Separator for the namespace key parts."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self.__validate_parts(self.prefix)

    # ....................... #

    def __validate_parts(self, *parts: StrKey) -> None:
        for p in parts:
            x = str(p)

            if not x:
                raise CoreError("Key part must be non-empty")

            if self.sep in x:
                raise CoreError(f"Key part must not contain separator '{self.sep}'")

    # ....................... #

    def key(self, *parts: StrKey) -> str:
        if not parts:
            raise CoreError("No parts provided")

        self.__validate_parts(self.prefix, *parts)

        str_parts = list(map(str, (self.prefix, *parts)))

        return self.sep.join(str_parts)
