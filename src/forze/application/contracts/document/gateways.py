"""Gateway port protocols for document persistence adapters."""

from typing import (
    Any,
    Awaitable,
    Generic,
    Never,
    Protocol,
    Sequence,
    TypeVar,
    overload,
)
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.document.value_objects import RowLockMode
from forze.application.contracts.querying import (
    AggregatesExpression,
    CursorPaginationExpression,
    QueryExpr,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec
from forze.domain.models import BaseDTO, Document

M = TypeVar("M", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)
D_co = TypeVar("D_co", bound=Document, covariant=True)
C_co = TypeVar("C_co", bound=BaseDTO, contravariant=True)
U_co = TypeVar("U_co", bound=BaseDTO, contravariant=True)

__all__ = [
    "DocumentReadGatewayPort",
    "DocumentWriteGatewayPort",
]


class DocumentReadGatewayPort(Protocol, Generic[M]):
    """Read gateway operations required by :class:`~forze.application.integrations.document.adapter.DocumentAdapter`."""

    @property
    def model_type(self) -> type[M]: ...

    @property
    def read_codec(self) -> ModelCodec[M, Any]:
        """Row decode/encode codec for this gateway's read model."""

        ...

    @property
    def tenant_aware(self) -> bool:
        """Whether the backing storage partitions rows by tenant."""

        ...

    # ....................... #

    def compile_filters(
        self,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    ) -> QueryExpr | None:
        """Parse *filters* once for reuse across count/list gateway calls."""
        ...

    # ....................... #

    def get(
        self,
        pk: UUID,
        *,
        for_update: RowLockMode = False,
    ) -> Awaitable[M]: ...

    # ....................... #

    def get_many(self, pks: Sequence[UUID]) -> Awaitable[list[M]]: ...

    # ....................... #

    @overload
    def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = ...,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> Awaitable[M | None]: ...

    @overload
    def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> Awaitable[T | None]: ...

    @overload
    def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> Awaitable[JsonDict | None]: ...

    @overload
    def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = ...,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Awaitable[Never]: ...

    def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = False,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Awaitable[M | T | JsonDict | None]: ...

    # ....................... #

    @overload
    def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_model: None = ...,
        return_fields: None = ...,
        parsed: QueryExpr | None = ...,
    ) -> Awaitable[list[JsonDict]]: ...

    @overload
    def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_model: type[T],
        return_fields: None = ...,
        parsed: QueryExpr | None = ...,
    ) -> Awaitable[list[T]]: ...

    @overload
    def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_model: None = ...,
        return_fields: None = ...,
        parsed: QueryExpr | None = ...,
    ) -> Awaitable[list[M]]: ...

    @overload
    def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_model: type[T],
        return_fields: None = ...,
        parsed: QueryExpr | None = ...,
    ) -> Awaitable[list[T]]: ...

    @overload
    def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
        parsed: QueryExpr | None = ...,
    ) -> Awaitable[list[JsonDict]]: ...

    @overload
    def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_model: type[T],
        return_fields: Sequence[str],
        parsed: QueryExpr | None = ...,
    ) -> Awaitable[Never]: ...

    def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        limit: int | None = None,
        offset: int | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        aggregates: AggregatesExpression | None = None,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
        parsed: QueryExpr | None = None,
    ) -> Awaitable[list[M] | list[T] | list[JsonDict]]: ...

    # ....................... #

    def find_many_aggregates(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        limit: int | None = None,
        offset: int | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        aggregates: AggregatesExpression,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
        parsed: QueryExpr | None = None,
    ) -> Awaitable[list[T] | list[JsonDict]]: ...

    # ....................... #

    def count_aggregates(
        self,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        *,
        aggregates: AggregatesExpression,
        parsed: QueryExpr | None = None,
    ) -> Awaitable[int]: ...

    # ....................... #

    @overload
    def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> Awaitable[list[M]]: ...

    @overload
    def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: type[T],
        return_fields: None = ...,
    ) -> Awaitable[list[T]]: ...

    @overload
    def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> Awaitable[list[JsonDict]]: ...

    @overload
    def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Awaitable[Never]: ...

    def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Awaitable[list[M] | list[T] | list[JsonDict]]: ...

    # ....................... #

    def count(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        parsed: QueryExpr | None = None,
    ) -> Awaitable[int]: ...


# ....................... #


class DocumentWriteGatewayPort(Protocol, Generic[D_co, C_co, U_co]):
    """Write gateway operations required by :class:`~forze.application.integrations.document.adapter.DocumentAdapter`."""

    def create(self, payload: C_co, *, id: UUID | None = None) -> Awaitable[D_co]: ...

    def create_many(
        self,
        payloads: Sequence[C_co],
        *,
        batch_size: int,
    ) -> Awaitable[Sequence[D_co]]: ...

    def ensure(self, id: UUID, payload: C_co) -> Awaitable[D_co]: ...

    def ensure_many(
        self,
        ids: Sequence[UUID],
        payloads: Sequence[C_co],
        *,
        batch_size: int,
    ) -> Awaitable[Sequence[D_co]]: ...

    def upsert(self, id: UUID, create: C_co, update: U_co) -> Awaitable[D_co]: ...

    def upsert_many(
        self,
        ids: Sequence[UUID],
        creates: Sequence[C_co],
        updates: Sequence[U_co],
        *,
        batch_size: int,
    ) -> Awaitable[Sequence[D_co]]: ...

    def update(
        self,
        pk: UUID,
        dto: U_co,
        *,
        rev: int | None = None,
    ) -> Awaitable[tuple[D_co, JsonDict]]: ...

    def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U_co],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int,
    ) -> Awaitable[tuple[Sequence[D_co], Sequence[JsonDict]]]: ...

    def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U_co,
        *,
        batch_size: int,
    ) -> Awaitable[tuple[int, Sequence[D_co]]]: ...

    def touch(self, pk: UUID) -> Awaitable[D_co]: ...

    def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        batch_size: int,
    ) -> Awaitable[Sequence[D_co]]: ...

    def kill(self, pk: UUID) -> Awaitable[None]: ...

    def kill_many(self, pks: Sequence[UUID], *, batch_size: int) -> Awaitable[None]: ...
