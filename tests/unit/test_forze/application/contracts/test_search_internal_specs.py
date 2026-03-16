"""Tests for forze.application.contracts.search.internal.specs."""

import pytest
from pydantic import BaseModel

from forze.base.errors import CoreError
from forze.application.contracts.search.internal.specs import (
    SearchFieldSpecInternal,
    SearchFuzzySpecInternal,
    SearchGroupSpecInternal,
    SearchIndexSpecInternal,
    SearchSpecInternal,
)


# ----------------------- #
# SearchGroupSpecInternal


class TestSearchGroupSpecInternal:
    def test_valid_group(self) -> None:
        g = SearchGroupSpecInternal(name="title", weight=2.0)
        assert g.name == "title"
        assert g.weight == 2.0

    def test_default_weight(self) -> None:
        g = SearchGroupSpecInternal(name="default")
        assert g.weight == 1.0

    def test_empty_name_raises(self) -> None:
        with pytest.raises(CoreError, match="cannot be empty"):
            SearchGroupSpecInternal(name="")

    def test_whitespace_name_raises(self) -> None:
        with pytest.raises(CoreError, match="cannot be empty"):
            SearchGroupSpecInternal(name="   ")

    def test_negative_weight_raises(self) -> None:
        with pytest.raises(CoreError, match="cannot be negative"):
            SearchGroupSpecInternal(name="g", weight=-1.0)

    def test_zero_weight_allowed(self) -> None:
        g = SearchGroupSpecInternal(name="g", weight=0.0)
        assert g.weight == 0.0

    def test_hints_default_empty(self) -> None:
        g = SearchGroupSpecInternal(name="g")
        assert g.hints == {}


# ----------------------- #
# SearchFieldSpecInternal


class TestSearchFieldSpecInternal:
    def test_valid_field(self) -> None:
        f = SearchFieldSpecInternal(path="title", group="main", weight=1.5)
        assert f.path == "title"
        assert f.group == "main"
        assert f.weight == 1.5

    def test_defaults(self) -> None:
        f = SearchFieldSpecInternal(path="name")
        assert f.group is None
        assert f.weight is None
        assert f.hints == {}

    def test_empty_path_raises(self) -> None:
        with pytest.raises(CoreError, match="cannot be empty"):
            SearchFieldSpecInternal(path="")

    def test_whitespace_path_raises(self) -> None:
        with pytest.raises(CoreError, match="cannot be empty"):
            SearchFieldSpecInternal(path="   ")

    def test_negative_weight_raises(self) -> None:
        with pytest.raises(CoreError, match="cannot be negative"):
            SearchFieldSpecInternal(path="x", weight=-0.1)

    def test_empty_group_name_raises(self) -> None:
        with pytest.raises(CoreError, match="cannot be empty"):
            SearchFieldSpecInternal(path="x", group="  ")

    def test_path_safe_strips_whitespace(self) -> None:
        f = SearchFieldSpecInternal(path=" title ")
        assert f.path_safe == "title"


# ----------------------- #
# SearchFuzzySpecInternal


class TestSearchFuzzySpecInternal:
    def test_defaults(self) -> None:
        f = SearchFuzzySpecInternal()
        assert f.enabled is False
        assert f.max_distance_ratio is None
        assert f.prefix_length is None

    def test_valid_fuzzy(self) -> None:
        f = SearchFuzzySpecInternal(
            enabled=True, max_distance_ratio=0.5, prefix_length=2
        )
        assert f.enabled is True
        assert f.max_distance_ratio == 0.5
        assert f.prefix_length == 2

    def test_negative_distance_ratio_raises(self) -> None:
        with pytest.raises(CoreError, match="cannot be negative"):
            SearchFuzzySpecInternal(max_distance_ratio=-0.1)

    def test_distance_ratio_above_1_raises(self) -> None:
        with pytest.raises(CoreError, match="greater than 1.0"):
            SearchFuzzySpecInternal(max_distance_ratio=1.1)

    def test_distance_ratio_at_boundaries(self) -> None:
        f0 = SearchFuzzySpecInternal(max_distance_ratio=0.0)
        assert f0.max_distance_ratio == 0.0
        f1 = SearchFuzzySpecInternal(max_distance_ratio=1.0)
        assert f1.max_distance_ratio == 1.0

    def test_negative_prefix_length_raises(self) -> None:
        with pytest.raises(CoreError, match="cannot be negative"):
            SearchFuzzySpecInternal(prefix_length=-1)

    def test_zero_prefix_length_allowed(self) -> None:
        f = SearchFuzzySpecInternal(prefix_length=0)
        assert f.prefix_length == 0


# ----------------------- #
# SearchIndexSpecInternal


class TestSearchIndexSpecInternal:
    def _field(self, path: str, group: str | None = None) -> SearchFieldSpecInternal:
        return SearchFieldSpecInternal(path=path, group=group)

    def _group(self, name: str) -> SearchGroupSpecInternal:
        return SearchGroupSpecInternal(name=name)

    def test_valid_index(self) -> None:
        idx = SearchIndexSpecInternal(
            fields=[self._field("title")],
        )
        assert len(idx.fields) == 1
        assert idx.mode == "fulltext"

    def test_no_fields_raises(self) -> None:
        with pytest.raises(CoreError, match="At least one field"):
            SearchIndexSpecInternal(fields=[])

    def test_duplicate_field_paths_raises(self) -> None:
        with pytest.raises(CoreError, match="must be unique"):
            SearchIndexSpecInternal(fields=[self._field("title"), self._field("title")])

    def test_default_group_not_in_groups_raises(self) -> None:
        with pytest.raises(CoreError, match="not found in groups"):
            SearchIndexSpecInternal(
                fields=[self._field("title")],
                default_group="missing",
            )

    def test_field_references_unknown_group_raises(self) -> None:
        with pytest.raises(CoreError, match="unknown group"):
            SearchIndexSpecInternal(
                fields=[self._field("title", group="bad")],
                groups=[self._group("good")],
                default_group="good",
            )

    def test_field_no_group_no_default_with_groups_raises(self) -> None:
        with pytest.raises(CoreError, match="no group and default_group is not set"):
            SearchIndexSpecInternal(
                fields=[self._field("title")],
                groups=[self._group("main")],
            )

    def test_field_with_default_group(self) -> None:
        idx = SearchIndexSpecInternal(
            fields=[self._field("title")],
            groups=[self._group("main")],
            default_group="main",
        )
        assert idx.default_group == "main"

    def test_groups_dict(self) -> None:
        g1 = self._group("a")
        g2 = self._group("b")
        idx = SearchIndexSpecInternal(
            fields=[self._field("x", group="a")],
            groups=[g1, g2],
            default_group="a",
        )
        assert idx.groups_dict == {"a": g1, "b": g2}

    def test_fuzzy_spec(self) -> None:
        fuzzy = SearchFuzzySpecInternal(enabled=True)
        idx = SearchIndexSpecInternal(
            fields=[self._field("x")],
            fuzzy=fuzzy,
        )
        assert idx.fuzzy is not None
        assert idx.fuzzy.enabled is True


# ----------------------- #
# SearchSpecInternal


class _TestModel(BaseModel):
    name: str


class TestSearchSpecInternal:
    def _index(self) -> SearchIndexSpecInternal:
        return SearchIndexSpecInternal(
            fields=[SearchFieldSpecInternal(path="name")],
        )

    def test_valid_spec(self) -> None:
        spec = SearchSpecInternal(
            namespace="test",
            model=_TestModel,
            indexes={"default": self._index()},
        )
        assert spec.namespace == "test"

    def test_no_indexes_raises(self) -> None:
        with pytest.raises(CoreError, match="At least one index"):
            SearchSpecInternal(
                namespace="test",
                model=_TestModel,
                indexes={},
            )

    def test_default_index_not_found_raises(self) -> None:
        with pytest.raises(CoreError, match="not found in indexes"):
            SearchSpecInternal(
                namespace="test",
                model=_TestModel,
                indexes={"idx": self._index()},
                default_index="missing",
            )

    def test_stable_default_index_uses_explicit(self) -> None:
        spec = SearchSpecInternal(
            namespace="test",
            model=_TestModel,
            indexes={"a": self._index(), "b": self._index()},
            default_index="b",
        )
        assert spec.stable_default_index == "b"

    def test_stable_default_index_uses_first_key(self) -> None:
        spec = SearchSpecInternal(
            namespace="test",
            model=_TestModel,
            indexes={"alpha": self._index()},
        )
        assert spec.stable_default_index == "alpha"

    def test_pick_index_default(self) -> None:
        spec = SearchSpecInternal(
            namespace="test",
            model=_TestModel,
            indexes={"main": self._index()},
            default_index="main",
        )
        name, idx = spec.pick_index()
        assert name == "main"

    def test_pick_index_with_options(self) -> None:
        idx1 = self._index()
        idx2 = self._index()
        spec = SearchSpecInternal(
            namespace="test",
            model=_TestModel,
            indexes={"a": idx1, "b": idx2},
            default_index="a",
        )
        name, idx = spec.pick_index({"use_index": "b"})
        assert name == "b"
        assert idx is idx2

    def test_pick_index_not_found_raises(self) -> None:
        spec = SearchSpecInternal(
            namespace="test",
            model=_TestModel,
            indexes={"a": self._index()},
            default_index="a",
        )
        with pytest.raises(CoreError, match="not found"):
            spec.pick_index({"use_index": "missing"})

    def test_pick_index_with_none_options(self) -> None:
        spec = SearchSpecInternal(
            namespace="test",
            model=_TestModel,
            indexes={"only": self._index()},
            default_index="only",
        )
        name, _ = spec.pick_index(None)
        assert name == "only"
