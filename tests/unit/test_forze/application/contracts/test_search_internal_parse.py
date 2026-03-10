"""Tests for forze.application.contracts.search.internal.parse."""

import pytest
from pydantic import BaseModel

from forze.base.errors import CoreError
from forze.application.contracts.search.internal.parse import (
    _parse_field_spec,
    _parse_fuzzy_spec,
    _parse_group_spec,
    _parse_index_spec,
    parse_search_spec,
)
from forze.application.contracts.search.specs import (
    SearchFieldSpec,
    SearchFuzzySpec,
    SearchGroupSpec,
    SearchIndexSpec,
    SearchSpec,
)

# ----------------------- #


class _M(BaseModel):
    name: str


class TestParseGroupSpec:
    def test_minimal(self) -> None:
        g = _parse_group_spec(SearchGroupSpec(name="g"))
        assert g.name == "g"
        assert g.weight == 1.0

    def test_with_weight(self) -> None:
        g = _parse_group_spec({"name": "g", "weight": 2.5})
        assert g.weight == 2.5


class TestParseFieldSpec:
    def test_minimal(self) -> None:
        f = _parse_field_spec(SearchFieldSpec(path="title"))
        assert f.path == "title"
        assert f.group is None
        assert f.weight is None

    def test_with_group_and_weight(self) -> None:
        f = _parse_field_spec({"path": "body", "group": "main", "weight": 0.8})
        assert f.group == "main"
        assert f.weight == 0.8


class TestParseFuzzySpec:
    def test_empty(self) -> None:
        f = _parse_fuzzy_spec({})
        assert f.enabled is False
        assert f.max_distance_ratio is None
        assert f.prefix_length is None

    def test_full(self) -> None:
        f = _parse_fuzzy_spec(
            SearchFuzzySpec(enabled=True, max_distance_ratio=0.3, prefix_length=1)
        )
        assert f.enabled is True
        assert f.max_distance_ratio == 0.3
        assert f.prefix_length == 1


class TestParseIndexSpec:
    def test_minimal(self) -> None:
        idx = _parse_index_spec(SearchIndexSpec(fields=[{"path": "name"}]))
        assert len(idx.fields) == 1
        assert idx.mode == "fulltext"

    def test_with_groups_and_fuzzy(self) -> None:
        idx = _parse_index_spec(
            {
                "fields": [{"path": "title", "group": "main"}],
                "groups": [{"name": "main"}],
                "default_group": "main",
                "fuzzy": {"enabled": True},
                "mode": "prefix",
                "source": "my_table",
            }
        )
        assert idx.mode == "prefix"
        assert idx.source == "my_table"
        assert idx.fuzzy is not None and idx.fuzzy.enabled

    def test_raise_if_no_sources(self) -> None:
        with pytest.raises(CoreError, match="must have a source"):
            _parse_index_spec(
                {"fields": [{"path": "x"}]},
                raise_if_no_sources=True,
            )

    def test_no_raise_when_source_present(self) -> None:
        idx = _parse_index_spec(
            {"fields": [{"path": "x"}], "source": "t"},
            raise_if_no_sources=True,
        )
        assert idx.source == "t"


class TestParseSearchSpec:
    def test_full_spec(self) -> None:
        spec = SearchSpec(
            namespace="test",
            model=_M,
            indexes={
                "primary": SearchIndexSpec(fields=[{"path": "name"}]),
            },
            default_index="primary",
        )
        internal = parse_search_spec(spec)
        assert internal.namespace == "test"
        assert "primary" in internal.indexes
        assert internal.default_index == "primary"

    def test_raise_if_no_sources(self) -> None:
        spec = SearchSpec(
            namespace="test",
            model=_M,
            indexes={"idx": SearchIndexSpec(fields=[{"path": "x"}])},
        )
        with pytest.raises(CoreError, match="must have a source"):
            parse_search_spec(spec, raise_if_no_sources=True)
