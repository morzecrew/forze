"""Unit tests for Mongo search cursor seek conditions."""

from typing import Any, cast

import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import read_fields_for_model
from forze.application.contracts.querying.pagination.cursor_token import (
    decode_keyset_v1,
)
from forze.application.contracts.search import SearchSpec
from forze_mongo.adapters.search._cursor_run import execute_mongo_ranked_cursor_search
from forze_mongo.adapters.search._cursor_seek import build_keyset_seek_match


def test_keyset_seek_after_desc_rank() -> None:
    match = build_keyset_seek_match(
        [("_mongo_rank", "desc"), ("id", "asc")],
        [0.9, "abc"],
        after=True,
    )

    assert "$or" in match
    branches = match["$or"]
    assert branches[0] == {"_mongo_rank": {"$lt": 0.9}}


# ....................... #


class _Doc(BaseModel):
    id: str
    score: int | None = None


class _FakeSearchGw:
    """Minimal stand-in for a MongoSearchGateway — enough for the non-ranked cursor path."""

    def __init__(self) -> None:
        self.model_type = _Doc
        self.spec = SearchSpec(name="s", model_type=_Doc, fields=("score",))
        self.read_fields = read_fields_for_model(_Doc)

    async def coll(self) -> str:
        return "c"

    def _from_storage_doc(self, r: dict[str, Any]) -> dict[str, Any]:
        return dict(r)

    def require_tenant_if_aware(self) -> None:
        return None

    def compile_filters(self, filters: Any) -> None:
        return None


class _FakeClient:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    async def aggregate(
        self, coll: Any, pipeline: Any, limit: Any = None
    ) -> list[dict[str, Any]]:
        return list(self._rows)


@pytest.mark.asyncio
async def test_non_ranked_cursor_preserves_non_default_null_placement() -> None:
    # A browse (non-ranked) search sorted asc NULLS LAST: the minted token must carry that
    # non-default null placement, or the next page is rejected as "Cursor does not match
    # current search sort".
    rows = [
        {"id": "a", "score": 1},
        {"id": "b", "score": 2},
        {"id": "c", "score": 3},
    ]
    gw = cast(Any, _FakeSearchGw())
    client = cast(Any, _FakeClient(rows))
    sorts = {"score": {"dir": "asc", "nulls": "last"}}

    async def _search(cursor: dict[str, Any]) -> Any:
        return await execute_mongo_ranked_cursor_search(
            gw,
            client=client,
            ranked_pipeline=[],
            terms=(),
            query="",
            filters=None,
            sorts=sorts,  # type: ignore[arg-type]
            cursor=cursor,
            return_type=None,
            return_fields=None,
        )

    page1 = await _search({"limit": 2})
    assert page1.next_cursor is not None

    # The emitted token embeds the active (non-default) null placement, not the canonical default.
    _keys, _dirs, tn, _vals = decode_keyset_v1(page1.next_cursor)
    assert tn == ["last", "first"]

    # Round-trips: the follow-up page validates instead of raising a sort mismatch.
    page2 = await _search({"after": page1.next_cursor, "limit": 2})
    assert page2 is not None
