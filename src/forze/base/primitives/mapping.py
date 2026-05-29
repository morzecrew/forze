"""Immutable mapping helpers for frozen integration configs."""

from types import MappingProxyType
from typing import Mapping

# ----------------------- #


def frozen_mapping[K, V](value: Mapping[K, V]) -> Mapping[K, V]:
    """Return an immutable view of ``value``."""

    if isinstance(value, MappingProxyType):
        return value

    return MappingProxyType(dict(value))
