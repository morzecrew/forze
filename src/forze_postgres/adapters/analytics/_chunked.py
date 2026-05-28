"""Chunked scan helpers for Postgres analytics."""

from __future__ import annotations

from typing import AsyncGenerator, Sequence, TypeVar

from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsRunOptions
from forze.application.contracts.analytics._adapter_common import dry_run_enabled
from forze.application.contracts.querying import PaginationExpression
from forze.base.serialization import pydantic_validate_many

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
        query_key: str,
        params: BaseModel,
        *,
        options: AnalyticsRunOptions | None,
        fetch_batch_size: int,
        row_type: type[BaseModel],
    ) -> AsyncGenerator[Sequence[BaseModel]]:
        host = self._host
        params = host._validated_params(query_key, params)  # type: ignore[protected-access]

        if dry_run_enabled(options):
            return

        max_rows = (options or {}).get("max_rows")
        cap = int(max_rows) if max_rows is not None else None
        batch = max(1, fetch_batch_size)
        offset = 0
        collected = 0
        buffer: list[BaseModel] = []

        while True:
            if cap is not None and collected >= cap:
                break

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

            typed = pydantic_validate_many(row_type, rows)
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
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[Sequence[R]]:
        del pagination
        host = self._host
        async for chunk in self._chunked_scan(
            query_key,
            params,
            options=options,
            fetch_batch_size=fetch_batch_size,
            row_type=host.spec.read,
        ):
            yield chunk  # type: ignore[misc]

    # ....................... #

    async def select_run_chunked(
        self,
        return_type: type[T],
        query_key: str,
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
            row_type=return_type,
        ):
            yield chunk  # type: ignore[misc]
