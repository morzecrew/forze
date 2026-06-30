"""Unit tests for the shared dotted-path projection primitives.

These back the cross-backend nested-projection behavior: a dotted path reshapes into a
nested output, sibling leaves merge, a requested root subsumes its leaves, and an absent
leaf is omitted (not set to ``None``). The mock oracle and every backend reshape through
:func:`build_projection`, so locking the semantics here locks them everywhere.
"""

from forze.base.primitives.projection import (
    MISSING,
    build_projection,
    path_get,
    projection_roots,
)

# ----------------------- #


def test_path_get_walks_dotted_path_and_flags_absent() -> None:
    doc = {"contract": {"reg_number": "X", "inner": {"v": 1}}, "top": None}

    assert path_get(doc, "contract.reg_number") == "X"
    assert path_get(doc, "contract.inner.v") == 1
    assert path_get(doc, "top") is None  # present None is not MISSING
    assert path_get(doc, "contract.nope") is MISSING
    assert path_get(doc, "top.x") is MISSING  # walking through a non-dict


def test_projection_roots_dedups_preserving_order() -> None:
    assert projection_roots(
        ["contract.reg_number", "contract.signed_at", "id", "contract"]
    ) == ("contract", "id")


def test_build_projection_none_returns_full_copy() -> None:
    doc = {"a": 1, "b": {"c": 2}}
    out = build_projection(doc, None)

    assert out == doc
    assert out is not doc  # shallow copy, not the same object


def test_build_projection_nests_and_merges_siblings() -> None:
    doc = {
        "contract": {"reg_number": "X", "signed_at": "2024", "extra": 1},
        "id": 7,
    }

    out = build_projection(doc, ["contract.reg_number", "contract.signed_at", "id"])

    assert out == {"contract": {"reg_number": "X", "signed_at": "2024"}, "id": 7}


def test_build_projection_root_subsumes_leaf() -> None:
    doc = {"contract": {"reg_number": "X", "extra": 1}}

    out = build_projection(doc, ["contract", "contract.reg_number"])

    assert out == {"contract": {"reg_number": "X", "extra": 1}}


def test_build_projection_skips_absent_leaf_keeps_present_none() -> None:
    doc = {"contract": {"reg_number": "X"}, "top": None}

    out = build_projection(doc, ["contract.bogus", "contract.reg_number", "top", "gone"])

    assert out == {"contract": {"reg_number": "X"}, "top": None}


def test_build_projection_deep_nested_path() -> None:
    doc = {"a": {"b": {"c": {"d": 9, "e": 10}}}}

    out = build_projection(doc, ["a.b.c.d"])

    assert out == {"a": {"b": {"c": {"d": 9}}}}


# ....................... #
# Array element projection: a dotted path through a list maps over each element,
# preserving structure (list of pruned sub-objects) and length (Mongo-aligned).


def test_build_projection_array_single_leaf_keeps_element_dicts() -> None:
    doc = {"items": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 1}]}

    out = build_projection(doc, ["items.sku"])

    assert out == {"items": [{"sku": "A"}, {"sku": "B"}]}


def test_build_projection_array_multi_leaf_merges_per_element() -> None:
    doc = {"items": [{"sku": "A", "qty": 2, "price": 10}, {"sku": "B", "qty": 1}]}

    out = build_projection(doc, ["items.sku", "items.qty"])

    assert out == {"items": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 1}]}


def test_build_projection_array_root_subsumes_leaf() -> None:
    doc = {"items": [{"sku": "A", "qty": 2}]}

    out = build_projection(doc, ["items", "items.sku"])

    assert out == {"items": [{"sku": "A", "qty": 2}]}


def test_build_projection_array_missing_in_element_preserves_length() -> None:
    doc = {"items": [{"sku": "A"}, {"qty": 9}]}

    out = build_projection(doc, ["items.sku"])

    # Length preserved: the element lacking ``sku`` yields an empty object, not a drop.
    assert out == {"items": [{"sku": "A"}, {}]}


def test_build_projection_nested_list_recurses() -> None:
    doc = {
        "orders": [
            {"items": [{"sku": "A", "x": 1}, {"sku": "C"}]},
            {"items": [{"sku": "B"}]},
        ]
    }

    out = build_projection(doc, ["orders.items.sku"])

    assert out == {
        "orders": [
            {"items": [{"sku": "A"}, {"sku": "C"}]},
            {"items": [{"sku": "B"}]},
        ]
    }


def test_build_projection_array_with_sibling_top_level_field() -> None:
    doc = {"id": 7, "items": [{"sku": "A", "qty": 2}]}

    out = build_projection(doc, ["id", "items.sku"])

    assert out == {"id": 7, "items": [{"sku": "A"}]}
