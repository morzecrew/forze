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

from forze.application.contracts.document.types import RowLockMode
from forze.application.contracts.querying import (
    AggregatesExpression,
    CursorPaginationExpression,
    QueryExpr,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

M = TypeVar("M", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)
D_co = TypeVar("D_co", bound=Document, covariant=True)
C_co = TypeVar("C_co", bound=CreateDocumentCmd, contravariant=True)
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
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = ...,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> M | None: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> T | None: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> JsonDict | None: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = ...,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never: ...

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = False,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> M | T | JsonDict | None: ...

    # ....................... #

    @overload
    async def find_many(
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
    ) -> list[JsonDict]: ...

    @overload
    async def find_many(
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
    ) -> list[T]: ...

    @overload
    async def find_many(
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
    ) -> list[M]: ...

    @overload
    async def find_many(
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
    ) -> list[T]: ...

    @overload
    async def find_many(
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
    ) -> list[JsonDict]: ...

    @overload
    async def find_many(
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
    ) -> Never: ...

    async def find_many(
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
    ) -> list[M] | list[T] | list[JsonDict]: ...

    # ....................... #

    async def find_many_aggregates(
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
    ) -> list[T] | list[JsonDict]: ...

    # ....................... #

    async def count_aggregates(
        self,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        *,
        aggregates: AggregatesExpression,
        parsed: QueryExpr | None = None,
    ) -> int: ...

    # ....................... #

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> list[M]: ...

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: type[T],
        return_fields: None = ...,
    ) -> list[T]: ...

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> list[JsonDict]: ...

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never: ...

    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> list[M] | list[T] | list[JsonDict]: ...

    # ....................... #

    async def count(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        parsed: QueryExpr | None = None,
    ) -> int: ...


# ....................... #


class DocumentWriteGatewayPort(Protocol, Generic[D_co, C_co, U_co]):
    """Write gateway operations required by :class:`~forze.application.integrations.document.adapter.DocumentAdapter`."""

    async def create(self, dto: C_co) -> D_co: ...

    async def create_many(
        self,
        dtos: Sequence[C_co],
        *,
        batch_size: int,
    ) -> Sequence[D_co]: ...

    async def ensure(self, dto: C_co) -> D_co: ...

    async def ensure_many(
        self,
        dtos: Sequence[C_co],
        *,
        batch_size: int,
    ) -> Sequence[D_co]: ...

    async def upsert(self, create_dto: C_co, update_dto: U_co) -> D_co: ...

    async def upsert_many(
        self,
        pairs: Sequence[tuple[C_co, U_co]],
        *,
        batch_size: int,
    ) -> Sequence[D_co]: ...

    async def update(
        self,
        pk: UUID,
        dto: U_co,
        *,
        rev: int | None = None,
    ) -> tuple[D_co, JsonDict]: ...

    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U_co],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int,
    ) -> tuple[Sequence[D_co], Sequence[JsonDict]]: ...

    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U_co,
        *,
        batch_size: int,
    ) -> tuple[int, Sequence[D_co]]: ...

    async def touch(self, pk: UUID) -> D_co: ...

    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        batch_size: int,
    ) -> Sequence[D_co]: ...

    async def kill(self, pk: UUID) -> None: ...

    async def kill_many(self, pks: Sequence[UUID], *, batch_size: int) -> None: ...
