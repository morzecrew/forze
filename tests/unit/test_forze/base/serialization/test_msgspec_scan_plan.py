"""Unknown-field pre-pass: precompiled scan plans + native ``forbid_unknown_fields`` skip."""

from __future__ import annotations

from typing import Any, Callable

import msgspec
import pytest

import forze.base.serialization.msgspec as msgspec_mod
from forze.base.serialization.msgspec import (
    _index_path,
    _mapping_path,
    _object_path,
    _raise_unknown_field,
    _strip_annotated,
    _struct_fields_cached,
    _struct_tree_forbids_unknown_fields,
    _type_may_contain_struct,
    _validate_no_unknown_fields,
    msgspec_decode_json_bytes,
    msgspec_validate,
    msgspec_validate_many,
)
from tests.support.codec_benchmark_models import CODEC_BENCHMARK_TIERS
from tests.support.codec_benchmark_msgspec_models import NestedCodecStruct

# ----------------------- #
# Legacy golden implementation (the pre-plan recursive walker, ported verbatim)
# ----------------------- #


def _legacy_validate_union(value: Any, tp: Any, path: str) -> None:
    from types import NoneType

    from typing import get_args

    args = get_args(tp)
    non_none = [arg for arg in args if arg is not NoneType]

    if len(non_none) == 1 and len(non_none) != len(args):
        _legacy_validate_no_unknown_fields(value, non_none[0], path)
        return

    structish = [arg for arg in args if _type_may_contain_struct(arg)]

    if not structish:
        return

    errors: list[msgspec.ValidationError] = []
    for arg in structish:
        try:
            _legacy_validate_no_unknown_fields(value, arg, path)
        except msgspec.ValidationError as exc:
            errors.append(exc)
        else:
            return

    if len(structish) == len(args) and errors:
        raise errors[0]


def _legacy_validate_no_unknown_fields(
    value: Any,
    expected_type: Any,
    path: str = "$",
) -> None:
    from types import UnionType

    from collections.abc import Mapping, Sequence
    from typing import Union, get_args, get_origin

    tp = _strip_annotated(expected_type)

    if value is None:
        return

    if isinstance(tp, type) and issubclass(tp, msgspec.Struct):
        if not isinstance(value, Mapping):
            return

        field_map = {field.encode_name: field for field in _struct_fields_cached(tp)}

        for key, child in value.items():
            key_str = str(key)
            field = field_map.get(key_str)

            if field is None:
                _raise_unknown_field(key_str, path)

            _legacy_validate_no_unknown_fields(
                child,
                field.type,
                _object_path(path, field.encode_name),
            )

        return

    origin = get_origin(tp)

    if origin in (Union, UnionType):
        _legacy_validate_union(value, tp, path)
        return

    if origin in (list, set, frozenset):
        args = get_args(tp)
        if not args or not isinstance(value, Sequence | set | frozenset):
            return

        inner = args[0]
        if not _type_may_contain_struct(inner):
            return

        for index, item in enumerate(value):
            _legacy_validate_no_unknown_fields(item, inner, _index_path(path, index))

        return

    if origin is tuple:
        args = get_args(tp)
        if not args or not isinstance(value, Sequence):
            return

        if len(args) == 2 and args[1] is Ellipsis:
            inner = args[0]
            if not _type_may_contain_struct(inner):
                return

            for index, item in enumerate(value):
                _legacy_validate_no_unknown_fields(item, inner, _index_path(path, index))

            return

        for index, (item, inner) in enumerate(zip(value, args, strict=False)):
            if _type_may_contain_struct(inner):
                _legacy_validate_no_unknown_fields(item, inner, _index_path(path, index))

        return

    if origin in (dict, Mapping):
        args = get_args(tp)
        if len(args) != 2 or not isinstance(value, Mapping):
            return

        inner = args[1]
        if not _type_may_contain_struct(inner):
            return

        for key, item in value.items():
            _legacy_validate_no_unknown_fields(item, inner, _mapping_path(path, key))


def _outcome(fn: Callable[[], None]) -> str | None:
    """Run ``fn``; return ``None`` on accept, the error message on reject."""

    try:
        fn()
    except msgspec.ValidationError as error:
        return str(error)

    return None


# ----------------------- #
# Goldens: parity with the legacy walker on the perf-suite tier models
# ----------------------- #


def _identity(row: dict[str, Any]) -> dict[str, Any]:
    return row


def _top_level_unknown(row: dict[str, Any]) -> dict[str, Any]:
    return {**row, "totally_unknown": 1}


def _nested_profile_unknown(row: dict[str, Any]) -> dict[str, Any]:
    return {**row, "profile": {**row["profile"], "sneaky": True}}


def _nested_line_unknown(row: dict[str, Any]) -> dict[str, Any]:
    lines = [dict(line) for line in row["lines"]]
    lines[1]["sneaky"] = 1
    return {**row, "lines": lines}


_TIER_MUTATIONS: list[Callable[[dict[str, Any]], dict[str, Any]]] = [
    _identity,
    _top_level_unknown,
]


@pytest.mark.parametrize("tier", CODEC_BENCHMARK_TIERS, ids=lambda t: t.name)
@pytest.mark.parametrize("mutate", _TIER_MUTATIONS, ids=lambda m: m.__name__)
def test_scan_plan_matches_legacy_on_tier_models(
    tier: Any,
    mutate: Callable[[dict[str, Any]], dict[str, Any]],
) -> None:
    """Same accept/reject decision and byte-identical message as the old walker."""

    rows = [mutate(dict(row)) for row in tier.sample_rows(4)]

    for index, row in enumerate(rows):
        path = _index_path("$", index)
        legacy = _outcome(
            lambda: _legacy_validate_no_unknown_fields(row, tier.msgspec_struct, path)
        )
        current = _outcome(
            lambda: _validate_no_unknown_fields(row, tier.msgspec_struct, path)
        )

        assert current == legacy


@pytest.mark.parametrize(
    "mutate",
    [_identity, _top_level_unknown, _nested_profile_unknown, _nested_line_unknown],
    ids=lambda m: m.__name__,
)
def test_scan_plan_matches_legacy_on_nested_tier_mutations(
    mutate: Callable[[dict[str, Any]], dict[str, Any]],
) -> None:
    nested_tier = next(t for t in CODEC_BENCHMARK_TIERS if t.name == "nested")
    row = mutate(dict(nested_tier.sample_rows(3)[2]))

    legacy = _outcome(lambda: _legacy_validate_no_unknown_fields(row, NestedCodecStruct))
    current = _outcome(lambda: _validate_no_unknown_fields(row, NestedCodecStruct))

    assert current == legacy
    if mutate is not _identity:
        assert current is not None


def test_scan_plan_rejection_messages_are_unchanged() -> None:
    nested_tier = next(t for t in CODEC_BENCHMARK_TIERS if t.name == "nested")
    row = dict(nested_tier.sample_rows(1)[0])

    top = _outcome(lambda: _validate_no_unknown_fields(_top_level_unknown(row), NestedCodecStruct))
    profile = _outcome(
        lambda: _validate_no_unknown_fields(_nested_profile_unknown(row), NestedCodecStruct)
    )
    line = _outcome(
        lambda: _validate_no_unknown_fields(_nested_line_unknown(row), NestedCodecStruct)
    )

    assert top == "Object contains unknown field `totally_unknown`"
    assert profile == "Object contains unknown field `sneaky` - at `$.profile`"
    assert line == "Object contains unknown field `sneaky` - at `$.lines[1]`"


# ----------------------- #
# Recursive structs: plan compilation terminates and deep unknowns reject
# ----------------------- #


class _TreeNode(msgspec.Struct):
    name: str
    children: list[_TreeNode] = msgspec.field(default_factory=list)


def test_recursive_struct_plan_does_not_hang_and_rejects_deep_unknown() -> None:
    valid = {"name": "root", "children": [{"name": "a", "children": []}]}
    _validate_no_unknown_fields(valid, _TreeNode)

    invalid = {
        "name": "root",
        "children": [{"name": "a", "children": [{"name": "b", "extra": 1}]}],
    }

    with pytest.raises(
        msgspec.ValidationError,
        match=r"Object contains unknown field `extra` - at `\$\.children\[0\]\.children\[0\]`",
    ):
        _validate_no_unknown_fields(invalid, _TreeNode)


# ----------------------- #
# Plan cache: compiled once per struct type
# ----------------------- #


def test_struct_scan_plan_compiled_once_per_type(monkeypatch: pytest.MonkeyPatch) -> None:
    class _CachedPlanStruct(msgspec.Struct):
        a: int
        child: _TreeNode | None = None

    compiled: list[type[msgspec.Struct]] = []
    real_compile = msgspec_mod._compile_struct_scan

    def spy(cls: type[msgspec.Struct], memo: dict[Any, Any]) -> Any:
        if cls not in msgspec_mod._STRUCT_SCAN_CACHE and cls not in memo:
            compiled.append(cls)
        return real_compile(cls, memo)

    monkeypatch.setattr(msgspec_mod, "_compile_struct_scan", spy)
    msgspec_mod._STRUCT_SCAN_CACHE.clear()

    for _ in range(3):
        msgspec_validate(_CachedPlanStruct, {"a": 1}, forbid_extra=True)
        msgspec_validate_many(
            _CachedPlanStruct,
            [{"a": 1}, {"a": 2}],
            forbid_extra=True,
        )

    assert compiled.count(_CachedPlanStruct) == 1
    assert compiled.count(_TreeNode) == 1


# ----------------------- #
# Native forbid_unknown_fields: pre-pass skip (P13a)
# ----------------------- #


class _ForbidChild(msgspec.Struct, forbid_unknown_fields=True):
    x: int


class _ForbidParent(msgspec.Struct, forbid_unknown_fields=True):
    child: _ForbidChild
    items: list[_ForbidChild] = msgspec.field(default_factory=list)


class _LaxChild(msgspec.Struct):
    x: int


class _MixedParent(msgspec.Struct, forbid_unknown_fields=True):
    child: _LaxChild


def _spy_prepass(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    calls: list[Any] = []
    real = msgspec_mod._validate_no_unknown_fields

    def spy(value: Any, expected_type: Any, path: str = "$") -> None:
        calls.append(expected_type)
        real(value, expected_type, path)

    monkeypatch.setattr(msgspec_mod, "_validate_no_unknown_fields", spy)
    return calls


def test_tree_forbid_predicate() -> None:
    assert _struct_tree_forbids_unknown_fields(_ForbidParent) is True
    assert _struct_tree_forbids_unknown_fields(_ForbidChild) is True
    assert _struct_tree_forbids_unknown_fields(_MixedParent) is False
    assert _struct_tree_forbids_unknown_fields(_LaxChild) is False
    assert _struct_tree_forbids_unknown_fields(_TreeNode) is False


def test_fully_forbidding_tree_skips_prepass_with_identical_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _spy_prepass(monkeypatch)

    ok = msgspec_validate(
        _ForbidParent,
        {"child": {"x": 1}, "items": [{"x": 2}]},
        forbid_extra=True,
    )
    assert ok == _ForbidParent(child=_ForbidChild(x=1), items=[_ForbidChild(x=2)])
    assert calls == []

    with pytest.raises(msgspec.ValidationError) as native_top:
        msgspec_validate(_ForbidParent, {"child": {"x": 1}, "e": 2}, forbid_extra=True)

    with pytest.raises(msgspec.ValidationError) as native_nested:
        msgspec_validate(
            _ForbidParent,
            {"child": {"x": 1, "e": 2}},
            forbid_extra=True,
        )

    assert calls == []
    assert str(native_top.value) == "Object contains unknown field `e`"
    assert str(native_nested.value) == "Object contains unknown field `e` - at `$.child`"

    # message parity with the Python pre-pass
    assert str(native_nested.value) == _outcome(
        lambda: _validate_no_unknown_fields({"child": {"x": 1, "e": 2}}, _ForbidParent)
    )


def test_fully_forbidding_tree_skips_prepass_in_many_and_json_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _spy_prepass(monkeypatch)

    many = msgspec_validate_many(
        _ForbidParent,
        [{"child": {"x": 1}}, {"child": {"x": 2}}],
        forbid_extra=True,
    )
    assert len(many) == 2

    decoded = msgspec_decode_json_bytes(
        _ForbidParent,
        b'{"child": {"x": 1}}',
        forbid_extra=True,
    )
    assert decoded.child.x == 1

    assert calls == []

    with pytest.raises(
        msgspec.ValidationError,
        match=r"Object contains unknown field `e` - at `\$\[1\]`",
    ):
        msgspec_validate_many(
            _ForbidParent,
            [{"child": {"x": 1}}, {"child": {"x": 2}, "e": 3}],
            forbid_extra=True,
        )

    with pytest.raises(
        msgspec.ValidationError,
        match="Object contains unknown field `e`",
    ):
        msgspec_decode_json_bytes(
            _ForbidParent,
            b'{"child": {"x": 1}, "e": 2}',
            forbid_extra=True,
        )

    assert calls == []


def test_mixed_tree_keeps_prepass_for_whole_tree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Conservative rule: any reachable non-forbidding struct keeps the full pre-pass."""

    calls = _spy_prepass(monkeypatch)

    ok = msgspec_validate(_MixedParent, {"child": {"x": 1}}, forbid_extra=True)
    assert ok == _MixedParent(child=_LaxChild(x=1))
    assert calls == [_MixedParent]

    # the pre-pass still rejects unknowns inside the non-forbidding child
    with pytest.raises(
        msgspec.ValidationError,
        match=r"Object contains unknown field `e` - at `\$\.child`",
    ):
        msgspec_validate(_MixedParent, {"child": {"x": 1, "e": 2}}, forbid_extra=True)

    assert calls == [_MixedParent, _MixedParent]
