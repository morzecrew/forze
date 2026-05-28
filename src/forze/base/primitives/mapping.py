"""Immutable mapping helpers for frozen integration configs."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import TypeVar

# ----------------------- #

K = TypeVar("K")
V = TypeVar("V")

# ....................... #


def frozen_mapping(value: Mapping[K, V]) -> Mapping[K, V]:
    """Return an immutable view of ``value``."""

    if isinstance(value, MappingProxyType):
        return value

    return MappingProxyType(dict(value))
