"""The converters, and the signature shape a type checker reads them through."""

import typing
from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType

import attrs
import pytest

from forze.base.primitives import MappingConverter, StrKeyMapping

# ----------------------- #

CONVERTERS = ("frozen", "to_str_key", "to_str_key_frozen")


class RouteName(StrEnum):
    ORDERS = "orders"


class SearchName(StrEnum):
    ORDERS = "orders"


@attrs.define(frozen=True, slots=True, kw_only=True)
class TwoMappingFields:
    """Two converter fields whose keys are two different enums.

    The shape that broke: ``attrs`` types each generated ``__init__`` parameter from the
    converter's input, so a key type variable in that signature is *shared* by both fields and
    solved once per constructor call. Two enums then contradict each other and a type checker
    refuses every argument but the first — while at runtime both have always worked, which is
    why nothing here caught it.
    """

    routes: StrKeyMapping[int] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    searches: StrKeyMapping[str] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )


# ....................... #


class TestConverterSignatures:
    """Two properties of the signature itself, because that is what ``attrs`` publishes.

    Neither is a style preference. A key type variable makes the class above unconstructible
    under a strict type checker, and an ``@overload`` pair makes it worse — pyright's ``attrs``
    support reads the *last* overload, whose parameter is ``None``, so the field accepts
    nothing else. Both are invisible to the runtime tests below and to ``mypy``, which cannot
    see these converters at all: every field using one carries a ``type: ignore[misc]`` for
    mypy's own "unsupported converter" limitation, and that suppresses the rest.
    """

    @pytest.mark.parametrize("name", CONVERTERS)
    def test_the_key_is_not_typed(self, name: str) -> None:
        hint = typing.get_type_hints(getattr(MappingConverter, name))["value"]
        # The hint is either the mapping itself (`frozen`) or a union with `None`.
        mapping_arm = next(
            arm
            for arm in (hint, *typing.get_args(hint))
            if isinstance(typing.get_origin(arm), type)
            and issubclass(typing.get_origin(arm), Mapping)
        )

        # `Mapping` is invariant in its key, so no named type — not even a union of `str` and
        # an enum — admits both `dict[str, V]` and `dict[SomeStrEnum, V]`.
        assert typing.get_args(mapping_arm)[0] is typing.Any

    @pytest.mark.parametrize("name", CONVERTERS)
    def test_the_converter_is_not_overloaded(self, name: str) -> None:
        assert typing.get_overloads(getattr(MappingConverter, name)) == []


# ....................... #


class TestTwoMappingFieldsTogether:
    def test_each_field_alone(self) -> None:
        assert TwoMappingFields(routes={RouteName.ORDERS: 1}).routes == {"orders": 1}
        assert TwoMappingFields(searches={SearchName.ORDERS: "a"}).searches == {"orders": "a"}

    def test_both_fields_at_once(self) -> None:
        both = TwoMappingFields(
            routes={RouteName.ORDERS: 1},
            searches={SearchName.ORDERS: "a"},
        )

        assert both.routes == {"orders": 1}
        assert both.searches == {"orders": "a"}

    def test_defaults_stay_none(self) -> None:
        assert TwoMappingFields().routes is None
        assert TwoMappingFields().searches is None


# ....................... #


class TestToStrKeyFrozen:
    def test_none_passthrough(self) -> None:
        assert MappingConverter.to_str_key_frozen(None) is None

    def test_plain_str_keys(self) -> None:
        assert MappingConverter.to_str_key_frozen({"a": 1}) == {"a": 1}

    def test_enum_keys_compare_equal_to_their_values(self) -> None:
        converted = MappingConverter.to_str_key_frozen({RouteName.ORDERS: 1})

        assert converted == {"orders": 1}
        assert converted is not None
        assert converted["orders"] == 1

    def test_the_result_is_read_only(self) -> None:
        converted = MappingConverter.to_str_key_frozen({"a": 1})

        assert isinstance(converted, MappingProxyType)

    def test_mutating_the_source_afterwards_does_not_reach_the_field(self) -> None:
        source = {"a": 1}
        converted = MappingConverter.to_str_key_frozen(source)
        source["b"] = 2

        assert converted == {"a": 1}

    def test_a_non_string_key_is_refused(self) -> None:
        with pytest.raises(TypeError, match="Expected str-compatible key, got int"):
            MappingConverter.to_str_key_frozen({1: "a"})

    def test_a_non_string_key_is_refused_through_a_field(self) -> None:
        # The runtime check is the only enforcement there is, so it has to fire where the
        # values actually arrive — `Any` in the signature means the checker will not.
        with pytest.raises(TypeError, match="Expected str-compatible key, got int"):
            TwoMappingFields(routes={1: 1})  # type: ignore[dict-item]


# ....................... #


class TestToStrKey:
    def test_none_passthrough(self) -> None:
        assert MappingConverter.to_str_key(None) is None

    def test_enum_keys_compare_equal_to_their_values(self) -> None:
        assert MappingConverter.to_str_key({RouteName.ORDERS: 1}) == {"orders": 1}

    def test_the_result_is_mutable_and_detached(self) -> None:
        source = {"a": 1}
        converted = MappingConverter.to_str_key(source)

        assert isinstance(converted, dict)
        source["b"] = 2
        assert converted == {"a": 1}

    def test_a_non_string_key_is_refused(self) -> None:
        with pytest.raises(TypeError, match="Expected str-compatible key, got tuple"):
            MappingConverter.to_str_key({("a",): 1})


# ....................... #


class TestFrozen:
    def test_a_copy_is_taken(self) -> None:
        source = {"a": 1}
        frozen = MappingConverter.frozen(source)
        source["b"] = 2

        assert frozen == {"a": 1}

    def test_a_proxy_is_copied_too(self) -> None:
        source = {"a": 1}
        frozen = MappingConverter.frozen(MappingProxyType(source))
        source["b"] = 2

        assert frozen == {"a": 1}

    def test_keys_are_not_checked(self) -> None:
        # `frozen` makes no claim about keys, which is why its return does not narrow them.
        assert MappingConverter.frozen({1: "a"}) == {1: "a"}
