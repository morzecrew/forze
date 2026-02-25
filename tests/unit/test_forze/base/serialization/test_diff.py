from forze.base.primitives import JsonDict
from forze.base.serialization.diff import (
    apply_dict_patch,
    calculate_dict_difference,
    deep_dict_intersection,
)


def test_apply_dict_patch_merges_nested_dicts() -> None:
    before: JsonDict = {"a": {"b": 1, "c": 2}}
    patch: JsonDict = {"a": {"c": 3, "d": 4}}

    after = apply_dict_patch(before, patch)
    assert after == {"a": {"b": 1, "c": 3, "d": 4}}


def test_calculate_dict_difference_simple_changes_and_additions() -> None:
    before: JsonDict = {"a": 1, "b": {"x": 1}}
    after: JsonDict = {"a": 2, "b": {"x": 1, "y": 3}, "c": 5}

    diff = calculate_dict_difference(before, after)
    # changed a, added b.y and c
    assert diff["a"] == 2
    assert diff["b"]["y"] == 3
    assert diff["c"] == 5


def test_deep_dict_intersection_returns_matching_leaf_paths() -> None:
    a: JsonDict = {"a": {"x": 1, "y": 2}, "b": 3}
    b: JsonDict = {"a": {"x": 10, "z": 3}, "b": 3}

    intersection = deep_dict_intersection(a, b)
    assert {("a", "x"), ("b",)}.issubset(intersection)

