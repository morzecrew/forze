"""Immutable mapping helpers for frozen integration configs."""

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Any, overload

from .types import StrKey

# ----------------------- #

type StrKeyMapping[V: Any] = Mapping[StrKey, V]
"""String-compatible mapping type."""

# ....................... #


class MappingConverter:
    @staticmethod
    def frozen[K, V](value: Mapping[K, V]) -> Mapping[K, V]:
        """A read-only view over a private copy of *value*.

        Always copied, including when *value* is already a ``MappingProxyType``. A proxy is
        read-only from the outside and says nothing about who else holds the dictionary
        behind it, so returning one unchanged would let its owner keep editing what a frozen
        object is meant to have settled — the field would look immutable and would not be.
        """

        return MappingProxyType(dict(value))

    # ....................... #

    @staticmethod
    def _validate_input_mapping[K: StrEnum, V](
        value: Mapping[str, V] | Mapping[K, V],
    ) -> None:
        for k in value:
            if not isinstance(k, StrKey):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise TypeError(f"Expected str-compatible key, got {type(k).__name__}")

    # ....................... #

    @overload
    @staticmethod
    def to_str_key[K: StrEnum, V: Any](
        value: Mapping[str, V] | Mapping[K, V],
    ) -> StrKeyMapping[V]: ...

    @overload
    @staticmethod
    def to_str_key[K: StrEnum, V: Any](value: None) -> None: ...

    @staticmethod
    def to_str_key[K: StrEnum, V: Any](
        value: Mapping[str, V] | Mapping[K, V] | None,
    ) -> StrKeyMapping[V] | None:
        if value is None:
            return None

        MappingConverter._validate_input_mapping(value)

        return dict(value)  # type: ignore[arg-type]

    # ....................... #

    @overload
    @staticmethod
    def to_str_key_frozen[K: StrEnum, V: Any](
        value: Mapping[str, V] | Mapping[K, V],
    ) -> StrKeyMapping[V]: ...

    @overload
    @staticmethod
    def to_str_key_frozen[K: StrEnum, V: Any](value: None) -> None: ...

    @staticmethod
    def to_str_key_frozen[K: StrEnum, V: Any](
        value: Mapping[str, V] | Mapping[K, V] | None,
    ) -> StrKeyMapping[V] | None:
        if value is None:
            return None

        MappingConverter._validate_input_mapping(value)

        return MappingConverter.frozen(dict(value))  # type: ignore[arg-type]
