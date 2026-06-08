"""Type-checker host protocol for Postgres analytics mixins."""

from typing import Any, AsyncGenerator, Awaitable, Callable, Protocol, Sequence, TypeVar

from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsRunOptions, AnalyticsSpec
from forze.application.contracts.base import CountlessPage, Page
from forze.application.contracts.querying import PaginationExpression
from forze.base.primitives import JsonDict, StrKey
from forze_postgres.execution.deps.configs import PostgresAnalyticsConfig
from forze_postgres.kernel.client import PostgresClientPort

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)

# ....................... #


class PostgresAnalyticsHost(Protocol[R, Ing]):
    """Methods and attrs required by analytics mixins beyond :class:`PostgresAnalyticsQueryMixin`."""

    client: PostgresClientPort
    spec: AnalyticsSpec[R, Ing]
    config: PostgresAnalyticsConfig

    # ....................... #

    def _validated_params(self, query_key: StrKey, params: BaseModel) -> BaseModel: ...

    def _ingest_qname(self) -> Awaitable[Any]: ...

    def _max_append_rows(self) -> int: ...

    def _cursor_column(self, query_key: StrKey) -> str | None: ...

    def _param_dict(self, params: BaseModel | JsonDict) -> dict[str, object]: ...

    def _fetch_rows(
        self,
        query_key: StrKey,
        params: BaseModel | JsonDict,
        *,
        options: AnalyticsRunOptions | None,
        limit: int | None,
        offset: int | None,
    ) -> Awaitable[list[JsonDict]]: ...

    def _run_with_timeout(
        self,
        options: AnalyticsRunOptions | None,
        fn: Callable[[], Awaitable[Any]],
    ) -> Awaitable[Any]: ...

    def _offset_page(
        self,
        query_key: StrKey,
        params: BaseModel,
        pagination: PaginationExpression | None,
        *,
        options: AnalyticsRunOptions | None,
        return_count: bool,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
    ) -> Awaitable[CountlessPage[Any] | Page[Any]]: ...

    def _cursor_page(
        self,
        query_key: StrKey,
        params: BaseModel,
        cursor: Any,
        *,
        options: AnalyticsRunOptions | None,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
    ) -> Awaitable[Any]: ...

    def _chunked_scan(
        self,
        query_key: StrKey,
        params: BaseModel,
        *,
        options: AnalyticsRunOptions | None,
        fetch_batch_size: int,
        _row_type: type[BaseModel],
    ) -> Awaitable[Any]: ...

    def run_chunked(
        self,
        query_key: StrKey,
        params: BaseModel,
        pagination: PaginationExpression | None,
        *,
        options: AnalyticsRunOptions | None,
        fetch_batch_size: int,
    ) -> AsyncGenerator[Sequence[R]]: ...
