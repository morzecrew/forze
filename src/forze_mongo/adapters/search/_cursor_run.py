"""Cursor pagination execution for Mongo ranked search."""

from __future__ import annotations

from typing import Any, Sequence

from pydantic import BaseModel

from forze.application.contracts.base import CursorPage
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    decode_keyset_v1,
    encode_keyset_v1,
    normalize_sorts_for_keyset,
    resolve_effective_sorts,
    row_value_for_sort_key,
)
from forze.application.contracts.search import ranked_search_cursor_key_spec
from forze.application.integrations.search import decrypt_search_rows
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_mongo.kernel.client.port import MongoClientPort

from ._cursor_seek import build_keyset_seek_match
from ._materialize import materialize_search_page
from ._pipeline import append_pagination_stages
from .base import MongoSearchGateway
from .constants import MONGO_RANK_FIELD

# ----------------------- #


async def execute_mongo_ranked_cursor_search[M: BaseModel](
    gw: MongoSearchGateway[M],
    *,
    client: MongoClientPort,
    ranked_pipeline: list[JsonDict],
    terms: tuple[str, ...],
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    cursor: CursorPaginationExpression | None,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
) -> CursorPage[Any]:
    """Run keyset cursor search over a ranked aggregation pipeline."""

    _ = query, filters
    c = dict(cursor or {})

    if c.get("after") and c.get("before"):
        raise exc.internal("Cursor pagination: pass at most one of 'after' or 'before'")

    lim: int = 10 if c.get("limit") is None else int(c["limit"])  # type: ignore[arg-type, call-overload]

    if lim < 1:
        raise exc.internal("Cursor pagination 'limit' must be positive")

    use_after = c.get("after") is not None
    use_before = c.get("before") is not None

    if terms:
        key_spec = ranked_search_cursor_key_spec(
            rank_field=MONGO_RANK_FIELD,
            sorts=sorts,
            read_fields=gw.read_fields,
        )
        nulls = ["first" if d == "asc" else "last" for _, d in key_spec]

    else:
        effective = resolve_effective_sorts(
            sorts=sorts,
            default_sort=gw.spec.default_sort,
            read_fields=gw.read_fields,
            spec_name=gw.spec.name,
            model=gw.model_type,
        )
        _norm = list(
            normalize_sorts_for_keyset(
                effective,
                read_fields=gw.read_fields,
                model=gw.model_type,
            )
        )
        key_spec = [(k, d) for k, d, _ in _norm]
        nulls = [n for _, _, n in _norm]

    sort_keys = [k for k, _ in key_spec]
    directions = [d for _, d in key_spec]

    pipeline = list(ranked_pipeline)

    if use_after or use_before:
        token = str(c["after" if use_after else "before"])
        tk, td, _tn, tv = decode_keyset_v1(token)

        if tk != sort_keys or len(td) != len(directions) or len(_tn) != len(nulls):
            raise exc.internal("Cursor does not match current search sort")

        for i, di in enumerate(directions):
            if (td[i] or "").lower() != di or (_tn[i] or "").lower() != nulls[i]:
                raise exc.internal("Cursor does not match current search sort")

        seek = build_keyset_seek_match(
            key_spec,
            list(tv),
            after=use_after and not use_before,
        )
        pipeline = [*pipeline, {"$match": seek}]

    fetch_limit = lim + 1
    data_pipeline = append_pagination_stages(
        pipeline,
        offset=0,
        limit=fetch_limit,
        strip_rank=False,
    )

    coll = await gw.coll()
    rows = await client.aggregate(coll, data_pipeline, limit=None)
    normalized = [
        gw._from_storage_doc(r) for r in rows  # pyright: ignore[reportPrivateUsage]
    ]

    if use_before:
        normalized = list(reversed(normalized))

    has_more = len(normalized) > lim
    page_rows_with_rank = normalized[:lim]

    def _encode_at(index: int) -> str | None:
        if index < 0 or index >= len(page_rows_with_rank):
            return None

        doc = page_rows_with_rank[index]
        vals = [row_value_for_sort_key(doc, k) for k in sort_keys]
        return encode_keyset_v1(
            sort_keys=sort_keys,
            directions=directions,
            values=vals,
        )

    next_c = (
        _encode_at(len(page_rows_with_rank) - 1)
        if has_more and page_rows_with_rank
        else None
    )
    prev_c = _encode_at(0) if page_rows_with_rank else None

    if use_before:
        next_c, prev_c = prev_c, next_c

    for doc in page_rows_with_rank:
        doc.pop(MONGO_RANK_FIELD, None)

    # Decrypt sealed fields out of the raw rows once, before materialization, so the spec
    # model, a custom return_type, and raw field projections all read plaintext.
    page_rows_with_rank, decode_codec = await decrypt_search_rows(
        gw.spec.resolved_read_codec, page_rows_with_rank
    )

    hits = materialize_search_page(
        page_rows=page_rows_with_rank,
        pool=None,
        u=0,
        page_limit=lim,
        return_type=return_type,
        return_fields=return_fields,
        model_type=gw.model_type,
        codec=decode_codec,
    )

    return CursorPage(
        hits=hits,
        next_cursor=next_c,
        prev_cursor=prev_c,
        has_more=has_more,
    )
