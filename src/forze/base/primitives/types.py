"""Common primitive types used across the library."""

from enum import StrEnum
from typing import Any, Mapping

# ----------------------- #

JsonDict = dict[str, Any]
"""JSON compatible dictionary type alias."""

StrKey = str | StrEnum
"""String-compatible key type alias."""

type StrKeyMapping[V: Any] = Mapping[StrKey, V]
"""String-compatible mapping type."""

# ....................... #


def str_key_mapping[K: StrKey, V: Any](
    value: Mapping[K, V] | None,
) -> StrKeyMapping[V] | None:
    """Return a string-compatible mapping of ``value``."""

    if value is None:
        return None

    return {key: item for key, item in value.items()}
