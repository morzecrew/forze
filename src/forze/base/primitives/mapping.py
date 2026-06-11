"""Immutable mapping helpers for frozen integration configs."""

from types import MappingProxyType
from typing import Any, Mapping, overload

from .types import StrKey

# ----------------------- #

type StrKeyMapping[V: Any] = Mapping[StrKey, V]
"""String-compatible mapping type."""

# ....................... #


class MappingConverter:
    @staticmethod
    def frozen[K, V](value: Mapping[K, V]) -> Mapping[K, V]:
        if isinstance(value, MappingProxyType):
            return value

        return MappingProxyType(dict(value))

    # ....................... #

    @overload
    @staticmethod
    def to_str_key[K: StrKey, V: Any](value: Mapping[K, V]) -> StrKeyMapping[V]: ...

    @overload
    @staticmethod
    def to_str_key[K: StrKey, V: Any](value: None) -> None: ...

    @staticmethod
    def to_str_key[K: StrKey, V: Any](
        value: Mapping[K, V] | None,
    ) -> StrKeyMapping[V] | None:
        if value is None:
            return None

        return dict(value)  # type: ignore[arg-type]

    # ....................... #

    @overload
    @staticmethod
    def to_str_key_frozen[K: StrKey, V: Any](
        value: Mapping[K, V],
    ) -> StrKeyMapping[V]: ...

    @overload
    @staticmethod
    def to_str_key_frozen[K: StrKey, V: Any](value: None) -> None: ...

    @staticmethod
    def to_str_key_frozen[K: StrKey, V: Any](
        value: Mapping[K, V] | None,
    ) -> StrKeyMapping[V] | None:
        if value is None:
            return None

        return MappingConverter.frozen(dict(value))  # type: ignore[arg-type]
