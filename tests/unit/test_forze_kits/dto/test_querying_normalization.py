"""Unit tests for bare empty-mapping normalization on list/search request DTOs."""

from __future__ import annotations

import pytest

from forze_kits.aggregates.document import (
    AggregatedListRequestDTO,
    CursorListRequestDTO,
    ListRequestDTO,
)
from forze_kits.aggregates.search import (
    CursorSearchRequestDTO,
    SearchRequestDTO,
)

# ----------------------- #

_LIST_DTOS = [ListRequestDTO, CursorListRequestDTO]


class TestEmptyMappingToNone:
    @pytest.mark.parametrize("dto_cls", _LIST_DTOS)
    def test_bare_empty_filter_and_sort_become_none(self, dto_cls):
        dto = dto_cls.model_validate({"filters": {}, "sorts": {}})

        assert dto.filters is None
        assert dto.sorts is None

    def test_aggregated_bare_empty_filter_becomes_none(self):
        dto = AggregatedListRequestDTO.model_validate(
            {"aggregates": {"$count": True}, "filters": {}, "sorts": {}},
        )

        assert dto.filters is None
        assert dto.sorts is None

    @pytest.mark.parametrize("dto_cls", [SearchRequestDTO, CursorSearchRequestDTO])
    def test_search_bare_empty_filter_and_sort_become_none(self, dto_cls):
        dto = dto_cls.model_validate({"query": "x", "filters": {}, "sorts": {}})

        assert dto.filters is None
        assert dto.sorts is None

    def test_real_filter_is_preserved(self):
        expr = {"$values": {"age": {"$gt": 18}}}
        dto = ListRequestDTO.model_validate({"filters": expr})

        assert dto.filters == expr

    def test_structured_but_empty_envelope_is_not_coerced(self):
        # ``{"$values": {}}`` is ambiguous (match-all vs. match-nothing); the DTO
        # leaves it intact so the strict filter parser still rejects it.
        expr = {"$values": {}}
        dto = ListRequestDTO.model_validate({"filters": expr})

        assert dto.filters == expr
