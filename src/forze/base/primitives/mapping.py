"""Immutable mapping helpers for frozen integration configs."""

from enum import StrEnum
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

    @staticmethod
    def _validate_input_mapping[V](
        value: Mapping[str, V] | Mapping[StrEnum, V],
    ) -> None:
        for k in value:
            if not isinstance(
                k, StrKey
            ):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise TypeError(f"Expected str-compatible key, got {type(k).__name__}")

    # ....................... #

    @overload
    @staticmethod
    def to_str_key[V: Any](
        value: Mapping[str, V] | Mapping[StrEnum, V],
    ) -> StrKeyMapping[V]: ...

    @overload
    @staticmethod
    def to_str_key[V: Any](value: None) -> None: ...

    @staticmethod
    def to_str_key[V: Any](
        value: Mapping[str, V] | Mapping[StrEnum, V] | None,
    ) -> StrKeyMapping[V] | None:
        if value is None:
            return None

        MappingConverter._validate_input_mapping(value)

        return dict(value)  # type: ignore[arg-type]

    # ....................... #

    @overload
    @staticmethod
    def to_str_key_frozen[V: Any](
        value: Mapping[str, V] | Mapping[StrEnum, V],
    ) -> StrKeyMapping[V]: ...

    @overload
    @staticmethod
    def to_str_key_frozen[V: Any](value: None) -> None: ...

    @staticmethod
    def to_str_key_frozen[V: Any](
        value: Mapping[str, V] | Mapping[StrEnum, V] | None,
    ) -> StrKeyMapping[V] | None:
        if value is None:
            return None

        MappingConverter._validate_input_mapping(value)

        return MappingConverter.frozen(dict(value))  # type: ignore[arg-type]
