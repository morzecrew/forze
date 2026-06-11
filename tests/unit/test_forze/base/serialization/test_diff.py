import random
from copy import deepcopy
from typing import Any

import pytest

from forze.base.primitives import JsonDict
from forze.base.serialization.diff import (
    _is_prefix,
    _set_nested,
    apply_dict_patch,
    calculate_dict_difference,
    has_hybrid_patch_conflict,
    is_prefix,
    split_touches_from_merge_patch,
)

# ----------------------- #
# apply_dict_patch


class TestApplyDictPatch:
    def test_merges_nested_dicts(self) -> None:
        before: JsonDict = {"a": {"b": 1, "c": 2}}
        patch: JsonDict = {"a": {"c": 3, "d": 4}}
        after = apply_dict_patch(before, patch)
        assert after == {"a": {"b": 1, "c": 3, "d": 4}}

    def test_does_not_mutate_original(self) -> None:
        before: JsonDict = {"a": 1, "b": {"x": 10}}
        original = deepcopy(before)
        apply_dict_patch(before, {"a": 2, "b": {"y": 20}})
        assert before == original

    def test_empty_patch_returns_copy(self) -> None:
        before: JsonDict = {"k": "v"}
        after = apply_dict_patch(before, {})
        assert after == before
        assert after is not before

    def test_top_level_key_addition(self) -> None:
        result = apply_dict_patch({"x": 1}, {"y": 2})
        assert result == {"x": 1, "y": 2}

    def test_top_level_key_override(self) -> None:
        result = apply_dict_patch({"x": 1}, {"x": 99})
        assert result == {"x": 99}

    def test_deep_nested_merge(self) -> None:
        before: JsonDict = {"a": {"b": {"c": 1}}}
        patch: JsonDict = {"a": {"b": {"d": 2}}}
        result = apply_dict_patch(before, patch)
        assert result == {"a": {"b": {"c": 1, "d": 2}}}

    def test_apply_patch_with_none_deletes_key(self) -> None:
        before: JsonDict = {"a": 1, "b": 2}
        patch: JsonDict = {"a": None}
        result = apply_dict_patch(before, patch)
        assert result == {"b": 2}

    def test_apply_patch_nested_none_deletes_key(self) -> None:
        before: JsonDict = {"a": {"x": 1, "y": 2}}
        patch: JsonDict = {"a": {"x": None}}
        result = apply_dict_patch(before, patch)
        assert result == {"a": {"y": 2}}

    def test_apply_patch_type_mismatch_scalar_to_dict(self) -> None:
        before: JsonDict = {"a": 1}
        patch: JsonDict = {"a": {"b": 2}}
        result = apply_dict_patch(before, patch)
        assert result == {"a": {"b": 2}}

    def test_apply_patch_type_mismatch_dict_to_scalar(self) -> None:
        before: JsonDict = {"a": {"b": 1}}
        patch: JsonDict = {"a": 2}
        result = apply_dict_patch(before, patch)
        assert result == {"a": 2}


# ----------------------- #
# calculate_dict_difference


class TestCalculateDictDifference:
    def test_simple_value_change(self) -> None:
        diff = calculate_dict_difference({"a": 1}, {"a": 2})
        assert diff == {"a": 2}

    def test_key_addition(self) -> None:
        diff = calculate_dict_difference({"a": 1}, {"a": 1, "b": 2})
        assert diff == {"b": 2}

    def test_key_deletion_as_none(self) -> None:
        diff = calculate_dict_difference({"a": 1, "b": 2}, {"a": 1})
        assert diff == {"b": None}

    def test_key_deletion_not_as_none(self) -> None:
        diff = calculate_dict_difference(
            {"a": 1, "b": 2}, {"a": 1}, deletions_as_none=False
        )
        assert "b" not in diff

    def test_nested_value_change(self) -> None:
        before: JsonDict = {"a": {"x": 1, "y": 2}}
        after: JsonDict = {"a": {"x": 1, "y": 99}}
        diff = calculate_dict_difference(before, after)
        assert diff == {"a": {"y": 99}}

    def test_nested_key_addition(self) -> None:
        before: JsonDict = {"a": {"x": 1}}
        after: JsonDict = {"a": {"x": 1, "z": 3}}
        diff = calculate_dict_difference(before, after)
        assert diff == {"a": {"z": 3}}

    def test_type_change(self) -> None:
        diff = calculate_dict_difference({"v": 1}, {"v": "string"})
        assert diff == {"v": "string"}

    def test_identical_dicts_produce_empty_diff(self) -> None:
        d: JsonDict = {"a": 1, "b": {"c": 2}}
        diff = calculate_dict_difference(d, deepcopy(d))
        assert diff == {}

    def test_list_item_addition(self) -> None:
        before: JsonDict = {"items": [1, 2]}
        after: JsonDict = {"items": [1, 2, 3]}
        diff = calculate_dict_difference(before, after)
        assert diff["items"] == [1, 2, 3]

    def test_list_item_removal(self) -> None:
        before: JsonDict = {"items": [1, 2, 3]}
        after: JsonDict = {"items": [1]}
        diff = calculate_dict_difference(before, after)
        assert diff["items"] == [1]

    def test_roundtrip_patch_restores_after(self) -> None:
        before: JsonDict = {"a": 1, "b": {"c": "old"}, "d": "old"}
        after: JsonDict = {"a": 1, "b": {"c": "new"}, "d": "new", "e": True}
        diff = calculate_dict_difference(before, after, deletions_as_none=False)
        restored = apply_dict_patch(before, diff)
        assert restored == after

    def test_roundtrip_with_deletions_as_none(self) -> None:
        before: JsonDict = {"a": 1, "b": 2}
        after: JsonDict = {"a": 1}
        diff = calculate_dict_difference(before, after, deletions_as_none=True)
        assert diff == {"b": None}
        restored = apply_dict_patch(before, diff)
        assert restored == after


# ----------------------- #
# Diff short-circuit parity


def _reference_diff(
    before: Any,
    after: Any,
    *,
    deletions_as_none: bool = True,
) -> JsonDict:
    """Pre-optimization diff walk (no subtree-equality short-circuit).

    Golden reference: the optimized ``calculate_dict_difference`` must agree
    with this on every input.
    """

    patch: JsonDict = {}

    def walk(b: Any, a: Any, path: tuple[str, ...]) -> None:
        if isinstance(b, dict) and isinstance(a, dict):
            for k in a:
                if k not in b:
                    _set_nested(patch, path + (k,), deepcopy(a[k]))
                else:
                    walk(b[k], a[k], path + (k,))

            if deletions_as_none:
                for k in b:
                    if k not in a:
                        _set_nested(patch, path + (k,), None)

            return

        if isinstance(b, list) and isinstance(a, list):
            if b != a:
                _set_nested(patch, path, deepcopy(a))

            return

        if b != a:
            _set_nested(
                patch,
                path,
                deepcopy(a) if isinstance(a, (dict, list, set, tuple)) else a,
            )

    walk(before, after, ())

    return patch


class TestDiffShortCircuitParity:
    """The subtree-equality fast path must be a behavioral no-op: optimized and
    reference walks agree on identical, partially-changed, and randomized
    nested structures."""

    def _random_value(self, rng: random.Random, depth: int) -> Any:
        kind = rng.randrange(6 if depth > 0 else 4)

        if kind == 0:
            return rng.randrange(100)
        if kind == 1:
            return rng.choice(["alpha", "beta", "gamma", ""])
        if kind == 2:
            return rng.choice([True, False, None])
        if kind == 3:
            return rng.random()
        if kind == 4:
            return [self._random_value(rng, depth - 1) for _ in range(rng.randrange(4))]

        return self._random_dict(rng, depth - 1)

    def _random_dict(self, rng: random.Random, depth: int) -> JsonDict:
        return {
            f"k{i}": self._random_value(rng, depth) for i in range(rng.randrange(1, 6))
        }

    def _mutate(self, rng: random.Random, node: JsonDict) -> None:
        """Apply a few random in-place mutations: change, delete, add, recurse."""

        for _ in range(rng.randrange(1, 4)):
            if not node:
                node["added"] = self._random_value(rng, 1)
                continue

            key = rng.choice(sorted(node))
            op = rng.randrange(4)

            if op == 0:
                node[key] = self._random_value(rng, 1)
            elif op == 1:
                del node[key]
            elif op == 2:
                node[f"new_{key}"] = self._random_value(rng, 1)
            elif isinstance(node[key], dict):
                self._mutate(rng, node[key])
            else:
                node[key] = self._random_value(rng, 1)

    @pytest.mark.parametrize("deletions_as_none", [True, False])
    def test_randomized_structures_match_reference(
        self, deletions_as_none: bool
    ) -> None:
        rng = random.Random(1337)

        for _ in range(100):
            before = self._random_dict(rng, depth=3)
            after = deepcopy(before)

            if rng.random() < 0.8:  # keep some exact no-op pairs in the mix
                self._mutate(rng, after)

            expected = _reference_diff(
                before, after, deletions_as_none=deletions_as_none
            )
            actual = calculate_dict_difference(
                before, after, deletions_as_none=deletions_as_none
            )

            assert actual == expected

    @pytest.mark.parametrize("deletions_as_none", [True, False])
    def test_handcrafted_cases_match_reference(self, deletions_as_none: bool) -> None:
        cases: list[tuple[JsonDict, JsonDict]] = [
            # equal subtree next to a changed one
            (
                {"same": {"a": 1, "b": [1, 2]}, "diff": {"x": 1}},
                {"same": {"a": 1, "b": [1, 2]}, "diff": {"x": 2}},
            ),
            # deletion below an otherwise-equal sibling
            (
                {"keep": {"deep": {"v": 1}}, "drop": {"a": 1, "b": 2}},
                {"keep": {"deep": {"v": 1}}, "drop": {"a": 1}},
            ),
            # type flips (dict→scalar, scalar→list)
            ({"a": {"b": 1}, "c": 1}, {"a": 2, "c": [1]}),
            # empty containers
            ({"a": {}, "b": []}, {"a": {}, "b": []}),
            ({"a": {}}, {"a": {"x": 1}}),
        ]

        for before, after in cases:
            assert calculate_dict_difference(
                before, after, deletions_as_none=deletions_as_none
            ) == _reference_diff(before, after, deletions_as_none=deletions_as_none)

    def test_shared_subtree_objects_short_circuit_to_empty_diff(self) -> None:
        # The same container object on both sides is by definition equal;
        # the fast path must yield an empty diff, like the full walk does.
        shared: JsonDict = {"x": {"y": [1, 2]}}
        before: JsonDict = {"a": shared, "b": 1}
        after: JsonDict = {"a": shared, "b": 1}

        assert calculate_dict_difference(before, after) == {}

    def test_diff_values_remain_isolated_copies(self) -> None:
        # Changed containers still land in the patch as deep copies, never as
        # references into `after`.
        before: JsonDict = {"a": {"b": [1]}}
        after: JsonDict = {"a": {"b": [1, 2]}, "c": {"d": 1}}

        diff = calculate_dict_difference(before, after)
        assert diff == {"a": {"b": [1, 2]}, "c": {"d": 1}}

        after["a"]["b"].append(3)
        after["c"]["d"] = 99
        assert diff["a"]["b"] == [1, 2]
        assert diff["c"]["d"] == 1


# ----------------------- #
# _is_prefix / is_prefix


class TestIsPrefix:
    def test_a_is_prefix_of_b(self) -> None:
        assert _is_prefix(("a",), ("a", "b")) is True

    def test_equal_paths_are_prefixes(self) -> None:
        assert _is_prefix(("a", "b"), ("a", "b")) is True

    def test_longer_is_not_prefix(self) -> None:
        assert _is_prefix(("a", "b", "c"), ("a",)) is False

    def test_disjoint_paths_are_not_prefixes(self) -> None:
        assert _is_prefix(("x",), ("y",)) is False

    def test_is_prefix_symmetric(self) -> None:
        assert is_prefix(("a",), ("a", "b")) is True
        assert is_prefix(("a", "b"), ("a",)) is True
        assert is_prefix(("x",), ("y",)) is False

    def test_empty_is_prefix_of_anything(self) -> None:
        assert _is_prefix((), ("a",)) is True
        assert _is_prefix((), ()) is True


# ----------------------- #
# split_touches_from_merge_patch


class TestSplitTouchesFromMergePatch:
    def test_separates_scalars_and_containers(self) -> None:
        patch: JsonDict = {"a": {"x": 1, "y": 2}, "b": 3}
        scalars, containers = split_touches_from_merge_patch(patch)
        assert ("b",) in scalars and scalars[("b",)] == 3
        assert ("a",) in containers

    def test_nested_dict_marked_as_container(self) -> None:
        patch: JsonDict = {"a": {"nested": {"deep": True}}}
        scalars, containers = split_touches_from_merge_patch(patch)
        assert ("a", "nested") in containers or ("a",) in containers

    def test_list_value_marked_as_container(self) -> None:
        patch: JsonDict = {"items": [1, 2, 3]}
        scalars, containers = split_touches_from_merge_patch(patch)
        assert ("items",) in containers

    def test_scalar_none_value(self) -> None:
        patch: JsonDict = {"deleted": None}
        scalars, containers = split_touches_from_merge_patch(patch)
        assert ("deleted",) in scalars
        assert scalars[("deleted",)] is None

    def test_empty_patch(self) -> None:
        scalars, containers = split_touches_from_merge_patch({})
        assert scalars == {}
        assert containers == set()

    def test_flat_all_scalars(self) -> None:
        patch: JsonDict = {"a": 1, "b": "x", "c": True}
        scalars, containers = split_touches_from_merge_patch(patch)
        assert len(scalars) == 3
        assert len(containers) == 0


# ----------------------- #
# has_hybrid_patch_conflict


class TestHasHybridPatchConflict:
    def test_no_conflict_disjoint_scalars(self) -> None:
        assert not has_hybrid_patch_conflict({("a",): 1}, set(), {("b",): 2}, set())

    def test_no_conflict_same_scalar_same_value(self) -> None:
        assert not has_hybrid_patch_conflict({("a",): 1}, set(), {("a",): 1}, set())

    def test_conflict_same_scalar_different_value(self) -> None:
        assert has_hybrid_patch_conflict({("a",): 1}, set(), {("a",): 2}, set())

    def test_conflict_container_overlap(self) -> None:
        assert has_hybrid_patch_conflict({}, {("a",)}, {}, {("a", "b")})

    def test_conflict_container_vs_scalar_prefix(self) -> None:
        assert has_hybrid_patch_conflict({}, {("a",)}, {("a", "x"): 1}, set())

    def test_conflict_scalar_vs_container_prefix(self) -> None:
        assert has_hybrid_patch_conflict({("a", "x"): 1}, set(), {}, {("a",)})

    def test_no_conflict_empty(self) -> None:
        assert not has_hybrid_patch_conflict({}, set(), {}, set())

    def test_conflict_nested_scalar_prefix(self) -> None:
        assert has_hybrid_patch_conflict({("a",): 1}, set(), {("a", "x"): 2}, set())

    def test_no_conflict_independent_containers(self) -> None:
        assert not has_hybrid_patch_conflict({}, {("a",)}, {}, {("b",)})
