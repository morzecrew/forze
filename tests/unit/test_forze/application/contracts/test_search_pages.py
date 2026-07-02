"""Search page value objects: the per-hit ``scores`` sidecar stays hit-aligned."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCountlessPage,
    SearchCursorPage,
    SearchPage,
)
from forze.base.exceptions import CoreException, ExceptionKind

pytestmark = pytest.mark.unit


class _Hit(BaseModel):
    id: str


_HITS = [_Hit(id="a"), _Hit(id="b")]


class TestScoresAlignment:
    def test_aligned_scores_accepted(self) -> None:
        page = SearchCountlessPage(hits=_HITS, page=1, size=2, scores=[0.9, 0.5])
        assert page.scores == [0.9, 0.5]

    def test_none_scores_accepted(self) -> None:
        assert SearchCountlessPage(hits=_HITS, page=1, size=2).scores is None

    @pytest.mark.parametrize(
        "page_factory",
        [
            lambda: SearchCountlessPage(hits=_HITS, page=1, size=2, scores=[0.9]),
            lambda: SearchPage(hits=_HITS, page=1, size=2, count=2, scores=[0.9]),
            lambda: SearchCursorPage(
                hits=_HITS,
                next_cursor=None,
                prev_cursor=None,
                has_more=False,
                scores=[0.9, 0.5, 0.1],
            ),
        ],
    )
    def test_misaligned_scores_rejected(self, page_factory) -> None:
        with pytest.raises(CoreException, match="index-aligned with hits") as ei:
            page_factory()

        assert ei.value.kind is ExceptionKind.INTERNAL
