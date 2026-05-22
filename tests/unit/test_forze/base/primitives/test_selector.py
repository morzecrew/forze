"""Tests for :mod:`forze.base.primitives.selector`."""

from enum import StrEnum

import pytest

from forze.base.errors import CoreError
from forze.base.primitives import StrKeySelector, str_key_selector
from forze.base.primitives.selector import _ExactKeys

# ----------------------- #

_sel = str_key_selector


def test_singleton_is_str_key_selector_instance() -> None:
    assert isinstance(_sel, StrKeySelector)
    assert str_key_selector is _sel


def test_all_keys_matches_any_key() -> None:
    sel = _sel.all_keys()
    assert _sel.matches(sel, "a")
    assert _sel.matches(sel, "projects.create")
    assert list(_sel.iter_matching(sel, ["x", "y"])) == ["x", "y"]


def test_exact_keys_single_and_multi() -> None:
    sel = _sel.exact("projects.create", "projects.update")
    assert _sel.matches(sel, "projects.create")
    assert _sel.matches(sel, "projects.update")
    assert not _sel.matches(sel, "projects.get")
    assert list(_sel.iter_matching(sel, ["projects.get", "projects.create"])) == [
        "projects.create",
    ]


def test_exact_accepts_str_enum() -> None:
    class K(StrEnum):
        CREATE = "create"

    sel = _sel.exact("document.create", K.CREATE)
    assert _sel.matches(sel, K.CREATE)
    assert _sel.matches(sel, "document.create")


def test_exact_empty_raises() -> None:
    with pytest.raises(CoreError, match="at least one key"):
        _sel.exact()


def test_exact_keys_empty_frozenset_raises() -> None:
    with pytest.raises(CoreError, match="non-empty"):
        _ExactKeys(keys=frozenset())


def test_prefix_matches_startswith() -> None:
    sel = _sel.prefix("projects")
    assert _sel.matches(sel, "projects.create")
    assert _sel.matches(sel, "projects-create")
    assert not _sel.matches(sel, "other.create")


def test_suffix_matches_endswith() -> None:
    sel = _sel.suffix(".create")
    assert _sel.matches(sel, "projects.create")
    assert not _sel.matches(sel, "orders-create")
    assert not _sel.matches(sel, "projects.get")

    assert _sel.matches(_sel.suffix("-create"), "orders-create")


def test_empty_prefix_suffix_glob_raise() -> None:
    with pytest.raises(CoreError, match="Prefix"):
        _sel.prefix("")
    with pytest.raises(CoreError, match="Suffix"):
        _sel.suffix("")
    with pytest.raises(CoreError, match="Glob"):
        _sel.glob("")


def test_glob_fnmatchcase() -> None:
    sel = _sel.glob("projects.*")
    assert _sel.matches(sel, "projects.create")
    assert _sel.matches(sel, "projects.create.v2")
    assert not _sel.matches(sel, "other.create")

    assert _sel.matches(_sel.glob("*"), "anything")


def test_when_predicate() -> None:
    sel = _sel.when(lambda k: k.endswith("_create"))
    assert _sel.matches(sel, "orders_create")
    assert not _sel.matches(sel, "orders.get")


def test_specificity_ordering() -> None:
    assert _sel.specificity(_sel.all_keys()) < _sel.specificity(_sel.suffix(".create"))
    assert _sel.specificity(_sel.suffix(".create")) < _sel.specificity(_sel.exact("a"))
    assert _sel.specificity(_sel.exact("a", "b")) > _sel.specificity(_sel.exact("a"))


def test_sort_by_specificity() -> None:
    ordered = _sel.sort_by_specificity(
        (
            _sel.exact("x"),
            _sel.all_keys(),
            _sel.suffix(".create"),
            _sel.when(lambda _: True),
        ),
    )
    assert ordered[0] == _sel.all_keys()
    assert ordered[-1] == _sel.exact("x")


def test_class_api_matches_singleton() -> None:
    direct = StrKeySelector()
    sel = direct.exact("a")
    assert direct.matches(sel, "a")
    assert direct.specificity(sel) == _sel.specificity(sel)
