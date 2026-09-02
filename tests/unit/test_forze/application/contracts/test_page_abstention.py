"""Page abstention reasons: only an empty page may say why it is empty."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from forze.application.contracts.base import (
    AbstentionReason,
    CountlessPage,
    CursorPage,
    Page,
    page_from_limit_offset,
)
from forze.application.contracts.search import (
    SearchCountlessPage,
    SearchCursorPage,
    SearchPage,
    search_page_from_limit_offset,
)
from forze.base.exceptions import CoreException, ExceptionKind

pytestmark = pytest.mark.unit


class TestAbstentionOnEmptyPage:
    @pytest.mark.parametrize("reason", ["no_match", "ambiguous", "not_permitted"])
    def test_empty_page_carries_reason(self, reason: AbstentionReason) -> None:
        page = CountlessPage[str](hits=[], page=1, size=1, abstention=reason)
        assert page.abstention == reason

    def test_default_is_none_everywhere(self) -> None:
        assert CountlessPage[str](hits=["a"], page=1, size=1).abstention is None
        assert Page[str](hits=["a"], page=1, size=1, count=1).abstention is None
        assert (
            CursorPage[str](hits=["a"], next_cursor=None, prev_cursor=None).abstention is None
        )

    @pytest.mark.parametrize(
        "page_factory",
        [
            lambda: CountlessPage(hits=["a"], page=1, size=1, abstention="no_match"),
            lambda: Page(hits=["a"], page=1, size=1, count=1, abstention="ambiguous"),
            lambda: CursorPage(
                hits=["a"],
                next_cursor=None,
                prev_cursor=None,
                abstention="not_permitted",
            ),
            lambda: SearchCountlessPage(hits=["a"], page=1, size=1, abstention="no_match"),
            lambda: SearchPage(hits=["a"], page=1, size=1, count=1, abstention="no_match"),
            lambda: SearchCursorPage(
                hits=["a"],
                next_cursor=None,
                prev_cursor=None,
                abstention="no_match",
            ),
        ],
    )
    def test_reason_on_non_empty_page_rejected(self, page_factory: Callable[[], Any]) -> None:
        with pytest.raises(CoreException, match="only valid on an empty page") as ei:
            page_factory()

        assert ei.value.kind is ExceptionKind.INTERNAL

    def test_search_pages_inherit_the_field(self) -> None:
        page = SearchCursorPage[str](
            hits=[],
            next_cursor=None,
            prev_cursor=None,
            abstention="not_permitted",
        )
        assert page.abstention == "not_permitted"


class TestBuildersPassReasonThrough:
    def test_base_builder(self) -> None:
        page: CountlessPage[str] = page_from_limit_offset([], {"limit": 10}, abstention="no_match")
        assert page.abstention == "no_match"

        counted: Page[str] = page_from_limit_offset(
            [], {"limit": 10}, total=0, abstention="not_permitted"
        )
        assert counted.abstention == "not_permitted"

    def test_search_builder(self) -> None:
        page: SearchCountlessPage[str] = search_page_from_limit_offset(
            [], {"limit": 10}, abstention="ambiguous"
        )
        assert page.abstention == "ambiguous"

        counted: SearchPage[str] = search_page_from_limit_offset(
            [], {"limit": 10}, total=0, abstention="no_match"
        )
        assert counted.abstention == "no_match"
