"""Immutable mapping helpers for frozen integration configs."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from .types import StrKey

# ----------------------- #

type StrKeyMapping[V: Any] = Mapping[StrKey, V]
"""String-compatible mapping type."""

# ....................... #


class MappingConverter:
    """Converters for mapping-valued fields on frozen configuration classes.

    Every method takes ``Mapping[Any, V]`` rather than naming the key type, and that is a
    deliberate choice with one reason: ``Mapping`` is **invariant** in its key. No static type
    admits both ``dict[str, V]`` and ``dict[SomeStrEnum, V]``, because neither is a subtype of
    the other under invariance — not their union either, since a union arm still has to match
    exactly. A key type here therefore refuses a caller's own ``StrEnum``-keyed dictionary,
    which is the shape a route-name enum produces and the main way these fields are populated.

    Worse, a key type variable *leaks*. ``attrs`` types the generated ``__init__`` parameter
    from the converter's input, so an unsolved ``K`` is shared by every field on the class that
    uses the same converter, and one constructor call solves it once. Two fields keyed by two
    different enums then contradict each other, and every argument but the first is rejected —
    a class whose fields are individually fine but cannot be passed together.

    Nothing is given up by writing ``Any``: :meth:`to_str_key` and :meth:`to_str_key_frozen`
    already check every key at runtime (:meth:`_validate_input_mapping` raises ``TypeError`` on
    a key that is not string-compatible), and that check was the only enforcement there ever
    was, since the static one could not be satisfied. The *field's* declared type still says
    what the mapping holds, and that is what a reader and a type checker both go by.
    """

    @staticmethod
    def frozen[V](value: Mapping[Any, V]) -> Mapping[Any, V]:
        """A read-only view over a private copy of *value*.

        Always copied, including when *value* is already a ``MappingProxyType``. A proxy is
        read-only from the outside and says nothing about who else holds the dictionary
        behind it, so returning one unchanged would let its owner keep editing what a frozen
        object is meant to have settled — the field would look immutable and would not be.

        Unlike the two converters below, this one makes no claim about the keys: it does not
        check them, so it does not narrow them either.
        """

        return MappingProxyType(dict(value))

    # ....................... #

    @staticmethod
    def _validate_input_mapping[V](value: Mapping[Any, V]) -> None:
        for k in value:
            if not isinstance(k, StrKey):
                raise TypeError(f"Expected str-compatible key, got {type(k).__name__}")

    # ....................... #

    @staticmethod
    def to_str_key[V](value: Mapping[Any, V] | None) -> StrKeyMapping[V] | None:
        """*value* with its keys checked to be string-compatible, as a mutable copy.

        Deliberately **not** overloaded on ``None``. An overload pair would narrow the return
        for a caller that passes a non-``None`` mapping, and it costs more than it pays: the
        ``attrs`` plugin in pyright reads the *last* overload's parameter type for the
        generated ``__init__``, which is ``None`` — so every field using this converter would
        accept nothing but ``None``. The one narrowing that mattered is unavailable anyway,
        since these fields are all declared optional.
        """

        if value is None:
            return None

        MappingConverter._validate_input_mapping(value)

        return dict(value)

    # ....................... #

    @staticmethod
    def to_str_key_frozen[V](value: Mapping[Any, V] | None) -> StrKeyMapping[V] | None:
        """*value* with its keys checked to be string-compatible, as a read-only view.

        :meth:`to_str_key` plus :meth:`frozen`, and not overloaded on ``None`` for the reason
        given there. One copy, not two: :meth:`frozen` takes it.
        """

        if value is None:
            return None

        MappingConverter._validate_input_mapping(value)

        return MappingConverter.frozen(value)
