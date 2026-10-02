"""Unit tests for Meilisearch search parameter helpers."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_meilisearch.adapters.search._offset_run import _MeilisearchOffsetHooks, page_order
from forze_meilisearch.adapters.search._search_params import (
    attributes_to_search_on,
    build_search_query_string,
    build_sort,
    render_user_sorts,
)
from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig


class _M(BaseModel):
    title: str
    body: str = ""


def test_build_search_query_any() -> None:
    assert build_search_query_string(("a", "b"), combine="any") == "a b"


def test_build_search_query_all() -> None:
    assert build_search_query_string(("a", "b"), combine="all") == '"a" "b"'


def test_build_search_query_strips_embedded_quotes() -> None:
    # An embedded ``"`` must not break phrase boundaries / split the query.
    assert build_search_query_string(('a"b', "c"), combine="all") == '"ab" "c"'
    assert build_search_query_string(('a"b', "c"), combine="any") == "ab c"


def test_attributes_to_search_on_fields_option() -> None:
    spec = SearchSpec(name="s", model_type=_M, fields=["title", "body"])
    attrs = attributes_to_search_on(spec, {"fields": ["title"]}, {})
    assert attrs == ["title"]


# ....................... #


class _Row(BaseModel):
    id: str
    title: str
    rank: int = 0


def _gw(*, sortable: list[str] | None = None, primary_key: str = "id") -> MagicMock:
    gw = MagicMock()
    gw.config = MeilisearchSearchConfig(
        index_uid="rows", sortable_attributes=sortable, primary_key=primary_key
    )
    gw.primary_key = primary_key
    return gw


_SPEC = SearchSpec(name="s", model_type=_Row, fields=["title"], default_sort={"rank": "desc"})


def page_sort(gw: Any, spec: SearchSpec[Any], sorts: Any, *, ranked: bool) -> list[str] | None:
    """The ``sort`` parameter the offset run sends for a page."""

    return build_sort(render_user_sorts(page_order(gw, spec, sorts, ranked=ranked), gw.config))


def test_a_blank_page_takes_the_default_sort_then_the_id() -> None:
    assert page_sort(_gw(), _SPEC, None, ranked=False) == ["rank:desc", "id:desc"]


def test_a_caller_sort_replaces_the_default() -> None:
    assert page_sort(_gw(), _SPEC, {"title": "asc"}, ranked=False) == ["title:asc", "id:asc"]


def test_a_page_with_search_text_sends_only_the_requests_sorts() -> None:
    # Meilisearch applies sort before exactness; anything added would outrank relevance ties.
    assert page_sort(_gw(), _SPEC, None, ranked=True) is None
    assert page_sort(_gw(), _SPEC, {"title": "asc"}, ranked=True) == ["title:asc"]


def test_the_id_sorts_by_the_primary_key() -> None:
    sort = page_sort(_gw(primary_key="doc_id"), _SPEC, {"id": "desc"}, ranked=False)

    assert sort == ["doc_id:desc"]
    assert page_sort(_gw(primary_key="doc_id"), _SPEC, None, ranked=False) == [
        "rank:desc",
        "doc_id:desc",
    ]


def test_a_sort_value_in_its_long_form_is_read() -> None:
    sort = page_sort(_gw(), _SPEC, {"title": {"dir": "desc"}}, ranked=False)

    assert sort == ["title:desc", "id:desc"]


def test_an_explicit_null_placement_is_refused() -> None:
    with pytest.raises(CoreException, match="nulls"):
        page_sort(_gw(), _SPEC, {"title": {"dir": "asc", "nulls": "last"}}, ranked=False)


def test_an_index_pinned_without_the_id_is_not_sorted_by_it() -> None:
    # The engine would refuse a sort on an attribute the index does not declare sortable.
    assert page_sort(_gw(sortable=["title", "rank"]), _SPEC, None, ranked=False) == ["rank:desc"]


# ....................... #


def _refusal(attribute: str) -> SimpleNamespace:
    # The engine names the refused attribute, then lists the sortable ones.
    return SimpleNamespace(
        code="invalid_search_sort",
        message=(
            f"Error message: Index `rows`: Attribute `{attribute}` is not sortable. "
            "Available sortable attributes are: `id`."
        ),
    )


def _hooks(spec_sort: tuple[str, ...]) -> _MeilisearchOffsetHooks:
    return _MeilisearchOffsetHooks(
        gw=MagicMock(),
        client=MagicMock(),
        query_string="",
        filter_str=None,
        attrs=None,
        sort_list=None,
        pagination_dict={},
        return_count=False,
        return_fields=None,
        spec_sort=spec_sort,
    )


def test_an_unsortable_attribute_the_request_named_is_the_callers_error() -> None:
    # ``id`` is in the spec's part of the sort and in the engine's list; ``title`` is refused.
    refusal = _hooks(("id",))._unsortable(_refusal("title"))  # pyright: ignore[reportPrivateUsage]

    assert refusal is not None and refusal.kind is ExceptionKind.PRECONDITION


def test_an_unsortable_attribute_the_spec_added_is_a_configuration_error() -> None:
    refusal = _hooks(("rank", "id"))._unsortable(_refusal("rank"))  # pyright: ignore[reportPrivateUsage]

    assert refusal is not None and refusal.kind is ExceptionKind.CONFIGURATION
    assert "ensure_index" in str(refusal)


def test_a_malformed_null_placement_keeps_the_canonical_error() -> None:
    # Read as a sort value first: a placement that is not one is malformed, not unsupported.
    # Federated native search renders a request's sorts here with nothing checking them first.
    with pytest.raises(CoreException) as refused:
        render_user_sorts({"title": {"dir": "asc", "nulls": "middle"}}, _gw().config)

    assert refused.value.code == "invalid_sort_value"


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("query", "expected"),
    [("python", {}), ("", {"rank": "desc", "id": "desc"})],
    ids=["ranked", "blank"],
)
async def test_the_snapshot_is_keyed_on_the_order_meilisearch_is_asked_for(
    monkeypatch: pytest.MonkeyPatch, query: str, expected: dict[str, str]
) -> None:
    # A ranked page sorts by the request alone, so a default sort it ignores is no part of the
    # key, and changing it leaves the ranked snapshots reusable.
    from forze_meilisearch.adapters.search import _offset_run

    executor = AsyncMock()
    monkeypatch.setattr(_offset_run, "execute_simple_offset_search_with_snapshot", executor)
    gw = _gw()
    gw.field_map = {}
    gw.build_filter = MagicMock(return_value=None)

    await _offset_run.execute_meilisearch_offset_search(
        gw,
        client=MagicMock(),
        query=query,
        filters=None,
        spec=_SPEC,
        variant="offset",
        fingerprint_extras=None,
        pagination={"limit": 5},
        snapshot=None,
        options=None,
        sorts=None,
        return_count=False,
        return_type=None,
        return_fields=None,
        result_snapshot=None,
    )

    assert executor.await_args.kwargs["fingerprint_sorts"] == expected


def test_another_engine_error_is_not_a_sort_refusal() -> None:
    other = SimpleNamespace(code="invalid_search_filter", message="Error message: bad filter")

    assert _hooks(("id",))._unsortable(other) is None  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
async def test_an_engine_error_that_is_no_sort_refusal_reaches_the_caller_unchanged() -> None:
    from meilisearch_python_sdk.errors import MeilisearchApiError

    error = MeilisearchApiError.__new__(MeilisearchApiError)
    error.code, error.message = "invalid_search_filter", "Error message: bad filter"
    index = MagicMock()
    index.search = AsyncMock(side_effect=error)
    hooks = _hooks(("id",))
    hooks.gw.config = MagicMock(max_total_hits=1000)
    hooks.gw._resolved_index_uid = AsyncMock(return_value="rows")  # pyright: ignore[reportPrivateUsage]
    hooks.client.index = MagicMock(return_value=index)

    with pytest.raises(MeilisearchApiError) as raised:
        await hooks.fetch_rows(MagicMock(fetch_offset=0, fetch_limit=5), want_snap=False)

    assert raised.value is error
