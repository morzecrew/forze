"""Chunked scan helpers for Postgres analytics."""

from typing import AsyncGenerator, Sequence, TypeVar

from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsRunOptions
from forze.application.contracts.querying import PaginationExpression
from forze.application.integrations.analytics.adapter_common import (
    decrypt_and_shape_rows,
    dry_run_enabled,
    validate_fetch_batch_size,
)
from forze.base.primitives import StrKey

from ._mixin_base import PostgresAnalyticsMixinBase

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)

# ....................... #


class PostgresAnalyticsChunkedMixin[R: BaseModel, Ing: BaseModel](
    PostgresAnalyticsMixinBase[R, Ing],
):
    """Batch streaming over offset-paginated analytics fetches."""

    async def _chunked_scan(
        self,
        query_key: StrKey,
        params: BaseModel,
        *,
        options: AnalyticsRunOptions | None,
        fetch_batch_size: int,
        return_type: type[BaseModel] | None,
    ) -> AsyncGenerator[Sequence[BaseModel]]:
        validate_fetch_batch_size(fetch_batch_size)
        host = self._host
        params = host._validated_params(query_key, params)  # type: ignore[protected-access]

        if dry_run_enabled(options):
            return

        max_rows = (options or {}).get("max_rows")
        cap = int(max_rows) if max_rows is not None else None
        batch = fetch_batch_size
        offset = 0
        collected = 0
        buffer: list[BaseModel] = []

        while cap is None or collected < cap:
            fetch_limit = batch

            if cap is not None:
                fetch_limit = min(batch, cap - collected)

            rows = await host._fetch_rows(  # type: ignore[protected-access]
                query_key,
                params,
                options=options,
                limit=fetch_limit,
                offset=offset,
            )

            if not rows:
                break

            typed = await decrypt_and_shape_rows(
                rows,
                read_codec=host.spec.resolved_read_codec,
                read_type=host.spec.read,
                return_type=return_type,
                return_fields=None,
            )
            collected += len(typed)
            offset += len(typed)
            buffer.extend(typed)

            while len(buffer) >= batch:
                yield buffer[:batch]
                buffer = buffer[batch:]

            if len(typed) < fetch_limit:
                break

        if buffer:
            yield buffer

    # ....................... #

    async def run_chunked(
        self,
        query_key: StrKey,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[Sequence[R]]:
        del pagination
        async for chunk in self._chunked_scan(
            query_key,
            params,
            options=options,
            fetch_batch_size=fetch_batch_size,
            return_type=None,
        ):
            yield chunk  # type: ignore[misc]

    # ....................... #

    async def select_run_chunked(
        self,
        return_type: type[T],
        query_key: StrKey,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[Sequence[T]]:
        del pagination
        async for chunk in self._chunked_scan(
            query_key,
            params,
            options=options,
            fetch_batch_size=fetch_batch_size,
            return_type=return_type,
        ):
            yield chunk  # type: ignore[misc]
