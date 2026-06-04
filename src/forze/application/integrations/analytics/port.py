"""Shared :class:`~forze.application.contracts.analytics.AnalyticsQueryPort` delegation.

Backends mix in :class:`AnalyticsQueryPortMixin` and implement only the
backend-specific hooks ``_offset_page`` / ``_cursor_page`` / ``run_chunked``.
The public ``AnalyticsQueryPort`` surface (run, projection, select, chunked, and
cursor variants) is delegated to those hooks here, so each warehouse integration
no longer re-declares the identical delegation boilerplate.
"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Sequence,
    TypeVar,
    cast,
)

from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsRunOptions
from forze.application.contracts.base import CountlessPage, CursorPage, Page
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
)
from forze.base.primitives import JsonDict, StrKey

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


class AnalyticsQueryPortMixin[R: BaseModel]:
    """Mixin delegating analytics query-port methods to backend page helpers.

    Concrete adapters provide ``_offset_page``, ``_cursor_page``, and
    ``run_chunked``; the eleven public delegation methods below are inherited.
    """

    spec: Any

    if TYPE_CHECKING:
        # Backend-provided hooks (declared for the type checker; concrete adapters
        # supply the implementations).

        async def _offset_page(
            self,
            query_key: StrKey,
            params: BaseModel,
            pagination: PaginationExpression | None,
            *,
            options: AnalyticsRunOptions | None,
            return_count: bool,
            return_type: type[BaseModel] | None,
            return_fields: Sequence[str] | None,
        ) -> CountlessPage[Any] | Page[Any]: ...

        async def _cursor_page(
            self,
            query_key: StrKey,
            params: BaseModel,
            cursor: CursorPaginationExpression | None,
            *,
            options: AnalyticsRunOptions | None,
            return_type: type[BaseModel] | None,
            return_fields: Sequence[str] | None,
        ) -> CursorPage[Any]: ...

        def run_chunked(
            self,
            query_key: StrKey,
            params: BaseModel,
            pagination: PaginationExpression | None = None,
            *,
            options: AnalyticsRunOptions | None = None,
            fetch_batch_size: int = 2000,
        ) -> AsyncGenerator[Sequence[R]]: ...

    # ....................... #

    async def run(
        self,
        query_key: StrKey,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> CountlessPage[R]:
        return await self._offset_page(
            query_key,
            params,
            pagination,
            options=options,
            return_count=False,
            return_type=None,
            return_fields=None,
        )

    # ....................... #

    async def run_page(
        self,
        query_key: StrKey,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Page[R]:
        return cast(
            Page[R],
            await self._offset_page(
                query_key,
                params,
                pagination,
                options=options,
                return_count=True,
                return_type=None,
                return_fields=None,
            ),
        )

    # ....................... #

    async def project_run(
        self,
        fields: Sequence[str],
        query_key: StrKey,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> CountlessPage[JsonDict]:
        return await self._offset_page(
            query_key,
            params,
            pagination,
            options=options,
            return_count=False,
            return_type=None,
            return_fields=fields,
        )

    # ....................... #

    async def project_run_page(
        self,
        fields: Sequence[str],
        query_key: StrKey,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Page[JsonDict]:
        return cast(
            Page[JsonDict],
            await self._offset_page(
                query_key,
                params,
                pagination,
                options=options,
                return_count=True,
                return_type=None,
                return_fields=fields,
            ),
        )

    # ....................... #

    async def project_run_chunked(
        self,
        fields: Sequence[str],
        query_key: StrKey,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[Sequence[JsonDict]]:
        async for chunk in self.run_chunked(
            query_key,
            params,
            pagination,
            options=options,
            fetch_batch_size=fetch_batch_size,
        ):
            yield [{k: row.model_dump().get(k) for k in fields} for row in chunk]

    # ....................... #

    async def select_run(
        self,
        return_type: type[T],
        query_key: StrKey,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> CountlessPage[T]:
        return await self._offset_page(
            query_key,
            params,
            pagination,
            options=options,
            return_count=False,
            return_type=return_type,
            return_fields=None,
        )

    # ....................... #

    async def select_run_page(
        self,
        return_type: type[T],
        query_key: StrKey,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Page[T]:
        return cast(
            Page[T],
            await self._offset_page(
                query_key,
                params,
                pagination,
                options=options,
                return_count=True,
                return_type=return_type,
                return_fields=None,
            ),
        )

    # ....................... #

    async def run_cursor(
        self,
        query_key: StrKey,
        params: BaseModel,
        cursor: CursorPaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> CursorPage[R]:
        return await self._cursor_page(
            query_key,
            params,
            cursor,
            options=options,
            return_type=None,
            return_fields=None,
        )

    # ....................... #

    async def project_run_cursor(
        self,
        fields: Sequence[str],
        query_key: StrKey,
        params: BaseModel,
        cursor: CursorPaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> CursorPage[JsonDict]:
        return await self._cursor_page(
            query_key,
            params,
            cursor,
            options=options,
            return_type=None,
            return_fields=fields,
        )

    # ....................... #

    async def select_run_cursor(
        self,
        return_type: type[T],
        query_key: StrKey,
        params: BaseModel,
        cursor: CursorPaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> CursorPage[T]:
        return await self._cursor_page(
            query_key,
            params,
            cursor,
            options=options,
            return_type=return_type,
            return_fields=None,
        )
