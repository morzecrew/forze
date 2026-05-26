"""Analytics query and ingest port definitions.

Named queries are registered on :class:`~.AnalyticsSpec`; adapters map ``query_key`` and
``params`` to warehouse SQL or API calls. Handlers must not pass raw SQL strings on these ports.

**Cursor runs (``run_cursor`` / ``project_run_cursor`` / ``select_run_cursor``):**
Adapters use opaque tokens in :class:`~forze.application.contracts.querying.CursorPaginationExpression`
(typically engine page tokens). Use offset methods such as :meth:`~AnalyticsQueryPort.run` when
cursors are unsupported.

**Total counts (``run_page``):** Adapters may raise exceptions when the engine cannot provide a
cheap total count.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Awaitable, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from ..base import CountlessPage, CursorPage, Page
from ..querying import CursorPaginationExpression, PaginationExpression
from .specs import AnalyticsSpec
from .types import AnalyticsRunOptions
from .value_objects import AnalyticsAppendResult

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)

# ....................... #


@runtime_checkable
class BaseAnalyticsPort(Protocol):
    """Shared ``spec`` binding for analytics adapters."""

    spec: AnalyticsSpec[Any, Any]
    """``AnalyticsSpec`` for this port instance."""


# ....................... #


class AnalyticsQueryPort[R: BaseModel](BaseAnalyticsPort, Protocol):
    """Named analytics queries with result shape encoded in method names.

    ``run*`` returns the spec read model ``R``; ``project_run*`` returns ``JsonDict`` rows;
    ``select_run*`` validates rows as ``return_type``. Methods without ``_page`` or ``_cursor``
    return :class:`~forze.application.contracts.base.CountlessPage` (no total count);
    ``*_page`` returns :class:`~forze.application.contracts.base.Page`;
    ``*_cursor`` returns :class:`~forze.application.contracts.base.CursorPage`;
    ``*_chunked`` yields batches for large scans.
    """

    def run(
        self,
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Awaitable[CountlessPage[R]]:
        """Execute a named query and return typed read models (no total count)."""
        ...  # pragma: no cover

    def run_page(
        self,
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Awaitable[Page[R]]:
        """Execute a named query with total matching count when supported."""
        ...  # pragma: no cover

    def run_chunked(
        self,
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncIterator[Sequence[R]]:
        """Execute a named query and yield row batches for large result sets."""
        ...  # pragma: no cover

    def project_run(
        self,
        fields: Sequence[str],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Awaitable[CountlessPage[JsonDict]]:
        """Named query with field projection (no total count)."""
        ...  # pragma: no cover

    def project_run_page(
        self,
        fields: Sequence[str],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Awaitable[Page[JsonDict]]:
        """Named query with projection and total count when supported."""
        ...  # pragma: no cover

    def project_run_chunked(
        self,
        fields: Sequence[str],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncIterator[Sequence[JsonDict]]:
        """Named query yielding projected row batches."""
        ...  # pragma: no cover

    def select_run(
        self,
        return_type: type[T],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Awaitable[CountlessPage[T]]:
        """Named query validating each row as ``return_type`` (no total count)."""
        ...  # pragma: no cover

    def select_run_page(
        self,
        return_type: type[T],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Awaitable[Page[T]]:
        """Named query as ``return_type`` with total count when supported."""
        ...  # pragma: no cover

    def select_run_chunked(
        self,
        return_type: type[T],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncIterator[Sequence[T]]:
        """Named query yielding batches validated as ``return_type``."""
        ...  # pragma: no cover

    def run_cursor(
        self,
        query_key: str,
        params: BaseModel,
        cursor: CursorPaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Awaitable[CursorPage[R]]:
        """Cursor page of typed read models for a named query."""
        ...  # pragma: no cover

    def project_run_cursor(
        self,
        fields: Sequence[str],
        query_key: str,
        params: BaseModel,
        cursor: CursorPaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Awaitable[CursorPage[JsonDict]]:
        """Cursor page with field projection."""
        ...  # pragma: no cover

    def select_run_cursor(
        self,
        return_type: type[T],
        query_key: str,
        params: BaseModel,
        cursor: CursorPaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Awaitable[CursorPage[T]]:
        """Cursor page validating each row as ``return_type``."""
        ...  # pragma: no cover


# ....................... #


class AnalyticsIngestPort[Ing: BaseModel](BaseAnalyticsPort, Protocol):
    """Append-only ingest into a pre-provisioned analytics table."""

    def append(self, rows: Sequence[Ing]) -> Awaitable[AnalyticsAppendResult | None]:
        """Append a batch of rows; return acceptance summary when available."""
        ...  # pragma: no cover
