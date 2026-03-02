from forze.base.primitives import JsonDict
from forze.base.serialization.diff import (
    apply_dict_patch,
    calculate_dict_difference,
    has_hybrid_patch_conflict,
    split_touches_from_merge_patch,
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


def test_split_touches_from_merge_patch_separates_scalars_and_containers() -> None:
    patch: JsonDict = {"a": {"x": 1, "y": 2}, "b": 3}
    scalars, containers = split_touches_from_merge_patch(patch)
    assert ("b",) in scalars and scalars[("b",)] == 3
    assert ("a",) in containers


def test_has_hybrid_patch_conflict_detects_prefix_overlap() -> None:
    a_scalars = {("a",): 1}
    a_containers = set()
    b_scalars = {("a", "x"): 2}
    b_containers = set()
    assert has_hybrid_patch_conflict(a_scalars, a_containers, b_scalars, b_containers)

