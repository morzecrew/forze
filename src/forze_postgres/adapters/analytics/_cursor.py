"""Cursor pagination for Postgres analytics."""

from __future__ import annotations

from typing import Any, Sequence, TypeVar

from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsRunOptions
from forze.application.contracts.base import CursorPage
from forze.application.contracts.querying import CursorPaginationExpression
from forze.application.integrations.analytics.adapter_common import (
    decrypt_and_shape_rows,
    dry_run_enabled,
    encode_keyset_cursor_next,
    encode_offset_cursor_next_prev,
    merge_forze_after_params,
    parse_keyset_cursor_after,
    parse_offset_cursor_after,
)
from forze.base.primitives import StrKey

from ._mixin_base import PostgresAnalyticsMixinBase

# ----------------------- #

_PG_BACKWARD_CURSOR = "Backward analytics cursors are not supported on PostgreSQL."

# ....................... #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)

# ....................... #


class PostgresAnalyticsCursorMixin[R: BaseModel, Ing: BaseModel](
    PostgresAnalyticsMixinBase[R, Ing],
):
    """Offset and keyset cursor pages over configured analytics SQL."""

    async def _cursor_page(
        self,
        query_key: StrKey,
        params: BaseModel,
        cursor: CursorPaginationExpression | None,
        *,
        options: AnalyticsRunOptions | None,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
    ) -> CursorPage[Any]:
        host = self._host
        params = host._validated_params(query_key, params)  # type: ignore[protected-access]

        if dry_run_enabled(options):
            return CursorPage(
                hits=[],
                next_cursor=None,
                prev_cursor=None,
                has_more=False,
            )

        if cursor_col := host._cursor_column(query_key):  # type: ignore[protected-access]
            after_value, lim = parse_keyset_cursor_after(
                cursor,
                backward_not_supported=_PG_BACKWARD_CURSOR,
            )
            bound = merge_forze_after_params(
                host._param_dict(params),  # type: ignore[protected-access]
                after_value,
            )
            rows = await host._fetch_rows(  # type: ignore[protected-access]
                query_key,
                bound,
                options=options,
                limit=lim,
                offset=None,
            )
            hits = await decrypt_and_shape_rows(
                rows,
                read_codec=host.spec.resolved_read_codec,
                read_type=host.spec.read,
                return_type=return_type,
                return_fields=return_fields,
            )
            next_c = encode_keyset_cursor_next(
                column=cursor_col,
                hits=hits,
                limit=lim,
            )
            prev_c = None

        else:
            start, lim = parse_offset_cursor_after(
                cursor,
                backward_not_supported=_PG_BACKWARD_CURSOR,
            )
            rows = await host._fetch_rows(  # type: ignore[protected-access]
                query_key,
                params,
                options=options,
                limit=lim,
                offset=start,
            )
            hits = await decrypt_and_shape_rows(
                rows,
                read_codec=host.spec.resolved_read_codec,
                read_type=host.spec.read,
                return_type=return_type,
                return_fields=return_fields,
            )
            next_c, prev_c = encode_offset_cursor_next_prev(
                start=start,
                page_len=len(hits),
                limit=lim,
            )

        return CursorPage(
            hits=hits,
            next_cursor=next_c,
            prev_cursor=prev_c,
            has_more=next_c is not None,
        )
