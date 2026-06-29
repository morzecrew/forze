"""Ports for document storage and retrieval."""

from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Literal,
    Protocol,
    Sequence,
    TypeVar,
    overload,
    runtime_checkable,
)
from uuid import UUID

from pydantic import BaseModel

from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO, Document

from ..base import CountlessPage, CursorPage, Page
from ..querying import (
    AggregatesExpression,
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from .specs import DocumentSpec
from .value_objects import KeyedCreate, KeyedUpdate, RowLockMode, UpsertItem

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

T = TypeVar("T", bound=BaseModel)

# ....................... #


class BaseDocumentPort(Protocol[R, D, C, U]):
    """Base port for document storage and retrieval."""

    spec: DocumentSpec[R, D, C, U]
    """Document specification."""

    # ....................... #

    @property
    def tenant_aware(self) -> bool:
        """Whether the backing storage partitions rows by tenant.

        Callers that depend on tenant isolation (e.g. authz grant resolution) can
        assert this is ``True`` to fail closed instead of querying across tenants.
        """
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class DocumentQueryPort(BaseDocumentPort[R, Any, Any, Any], Protocol[R]):
    """Query operations for document aggregates.

    Result shape is encoded in the method name: ``get*`` / ``find*`` return the
    read model ``R``; ``project*`` returns ``JsonDict`` rows; ``select*`` uses an
    explicit ``return_type``; ``*_many`` is countless offset pagination;
    ``*_page`` includes a total count; ``*_cursor`` is keyset pagination;
    ``aggregate_*`` returns aggregate rows as JSON; ``select_*_aggregated``
    validates aggregate rows against ``return_type``; ``*_stream`` yields keyset
    batches for large exports.

    ``for_update`` uses :data:`~forze.application.contracts.document.RowLockMode`.
    Postgres honors ``"nowait"`` and ``"skip_locked"``; other backends treat
    those modes as ``True`` (transaction required) and log at debug level.
    """

    def get(
        self,
        pk: UUID,
        *,
        for_update: RowLockMode = False,
        skip_cache: bool = False,
    ) -> Awaitable[R]:
        """Fetch a single document by primary key as the typed read model."""
        ...  # pragma: no cover

    def get_many(
        self,
        pks: Sequence[UUID],
        *,
        skip_cache: bool = False,
    ) -> Awaitable[Sequence[R]]:
        """Fetch multiple documents by primary key as typed read models."""
        ...  # pragma: no cover

    def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = False,
    ) -> Awaitable[R | None]:
        """Find a single document by filters or return ``None`` when missing."""
        ...  # pragma: no cover

    def project(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        fields: Sequence[str],
        *,
        for_update: RowLockMode = False,
    ) -> Awaitable[JsonDict | None]:
        """Find a single document by filters and project ``fields`` to a JSON mapping."""
        ...  # pragma: no cover

    def select(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        return_type: type[T],
        *,
        for_update: RowLockMode = False,
    ) -> Awaitable[T | None]:
        """Find a single document by filters and validate as ``return_type``."""
        ...  # pragma: no cover

    def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CountlessPage[R]]:
        """List documents (offset pagination) without a total count query."""
        ...  # pragma: no cover

    def project_many(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CountlessPage[JsonDict]]:
        """List documents with field projection (no total count query)."""
        ...  # pragma: no cover

    def select_many(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CountlessPage[T]]:
        """List documents validating each row as ``return_type`` (no total count)."""
        ...  # pragma: no cover

    def find_page(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[Page[R]]:
        """List documents with offset pagination and total matching row count."""
        ...  # pragma: no cover

    def project_page(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[Page[JsonDict]]:
        """List documents with projection and total matching row count."""
        ...  # pragma: no cover

    def select_page(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[Page[T]]:
        """List documents as ``return_type`` with total matching row count."""
        ...  # pragma: no cover

    def find_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CursorPage[R]]:
        """Keyset / cursor page of typed read models (opaque ``prev`` / ``next`` cursors)."""
        ...  # pragma: no cover

    def project_cursor(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CursorPage[JsonDict]]:
        """Keyset / cursor page with field projection."""
        ...  # pragma: no cover

    def select_cursor(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CursorPage[T]]:
        """Keyset / cursor page validating each row as ``return_type``."""
        ...  # pragma: no cover

    def find_stream(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        sorts: QuerySortExpression | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[R]]:
        """Yield keyset batches of read models for large exports (no total count)."""
        ...  # pragma: no cover

    def project_stream(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        sorts: QuerySortExpression | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[JsonDict]]:
        """Yield keyset batches with field projection for large exports."""
        ...  # pragma: no cover

    def select_stream(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        sorts: QuerySortExpression | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[T]]:
        """Yield keyset batches validated as ``return_type`` for large exports."""
        ...  # pragma: no cover

    def aggregate_many(
        self,
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CountlessPage[JsonDict]]:
        """Aggregate query returning JSON rows (no total count query)."""
        ...  # pragma: no cover

    def aggregate_page(
        self,
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[Page[JsonDict]]:
        """Aggregate query returning JSON rows and total group count."""
        ...  # pragma: no cover

    def select_many_aggregated(
        self,
        return_type: type[T],
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CountlessPage[T]]:
        """Aggregate query validating each row as ``return_type`` (no total count)."""
        ...  # pragma: no cover

    def select_page_aggregated(
        self,
        return_type: type[T],
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[Page[T]]:
        """Aggregate query with typed rows and total group count."""
        ...  # pragma: no cover

    def count(self, filters: QueryFilterExpression | None = None) -> Awaitable[int]:  # type: ignore[valid-type]
        """Count documents by filters."""
        ...  # pragma: no cover

    def with_parameters(self, params: BaseModel) -> "DocumentQueryPort[R]":
        """Bind typed query parameters, returning a param-bound clone of this port.

        The spec must declare a :attr:`~forze.application.contracts.document.DocumentSpec.query_params`
        contract; *params* must be an instance of it. A supporting backend applies the values as
        query-scoped session settings the underlying relation reads internally, and the read DSL
        composes on top unchanged. Raises if the spec declares no contract, *params* is the wrong
        type, or the backend does not support query parameters.
        """
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class DocumentCommandPort(BaseDocumentPort[R, D, C, U], Protocol[R, D, C, U]):
    """Command operations for document aggregates."""

    @overload
    def create(
        self,
        payload: C,
        *,
        id: UUID | None = None,
        return_new: Literal[True] = True,
    ) -> Awaitable[R]:
        """Create a new document from *payload* (server id unless ``id`` is given)."""
        ...  # pragma: no cover

    @overload
    def create(
        self,
        payload: C,
        *,
        id: UUID | None = None,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Create a new document from *payload* (server id unless ``id`` is given)."""
        ...  # pragma: no cover

    def create(
        self,
        payload: C,
        *,
        id: UUID | None = None,
        return_new: bool = True,
    ) -> Awaitable[R | None]:
        """Create a document from *payload* (domain fields only).

        The primary key is server-generated unless ``id`` is supplied — a caller-chosen
        "put" that inserts and fails on a primary-key conflict.
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def create_many(
        self,
        payloads: Sequence[C],
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[Sequence[R]]:
        """Create multiple documents in a batch."""
        ...  # pragma: no cover

    @overload
    def create_many(
        self,
        payloads: Sequence[C],
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Create multiple documents in a batch."""
        ...  # pragma: no cover

    def create_many(
        self,
        payloads: Sequence[C],
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[R] | None]:
        """Create multiple documents in a batch."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def ensure(
        self,
        id: UUID,
        payload: C,
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[R]:
        """Insert *payload* at *id* when missing; if it exists, return it unchanged."""
        ...  # pragma: no cover

    @overload
    def ensure(
        self,
        id: UUID,
        payload: C,
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Insert when missing; no read when ``return_new`` is false."""
        ...  # pragma: no cover

    def ensure(
        self,
        id: UUID,
        payload: C,
        *,
        return_new: bool = True,
    ) -> Awaitable[R | None]:
        """Insert *payload* at primary key *id* when missing; return it unchanged on conflict.

        Idempotent by primary key — **insert-only on conflict**; existing rows are never
        mutated. To preserve ``created_at``/``last_update_at`` on import, pass a payload
        carrying those fields (the ``forze_kits`` import-timestamps mixin); otherwise the
        server stamps them. Gateways may hydrate the returned domain row from the write
        payload when read and write share the same physical source.
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def ensure_many(
        self,
        items: Sequence[KeyedCreate[C]],
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[Sequence[R]]:
        """Bulk insert-when-missing; existing primary keys are left unchanged."""
        ...  # pragma: no cover

    @overload
    def ensure_many(
        self,
        items: Sequence[KeyedCreate[C]],
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Bulk insert-when-missing without re-reads."""
        ...  # pragma: no cover

    def ensure_many(
        self,
        items: Sequence[KeyedCreate[C]],
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[R] | None]:
        """Bulk insert-when-missing, keyed by each :class:`KeyedCreate.id`.

        Ids must be unique within ``items``. Order of the returned read models matches
        ``items``. Existing primary keys are left unchanged.
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def upsert(
        self,
        id: UUID,
        create: C,
        update: U,
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[R]:
        """Insert *create* at *id*, or apply *update* if a row with that id exists."""
        ...  # pragma: no cover

    @overload
    def upsert(
        self,
        id: UUID,
        create: C,
        update: U,
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Insert or update without a follow-up read when ``return_new`` is false."""
        ...  # pragma: no cover

    def upsert(
        self,
        id: UUID,
        create: C,
        update: U,
        *,
        return_new: bool = True,
    ) -> Awaitable[R | None]:
        """Insert *create* at primary key *id* when missing; on conflict apply *update* like :meth:`update`.

        The update branch uses the current stored revision (same optimistic rules as
        :meth:`update`). This is **not** a Mongo replace-all upsert — conflict rows are
        patched via domain apply. Gateways may hydrate insert results from the write
        payload when read and write share the same physical source.
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def upsert_many(
        self,
        items: Sequence[UpsertItem[C, U]],
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[Sequence[R]]:
        """Bulk upsert: each :class:`UpsertItem` carries ``id`` + create + update payloads."""
        ...  # pragma: no cover

    @overload
    def upsert_many(
        self,
        items: Sequence[UpsertItem[C, U]],
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Bulk upsert without re-reads when ``return_new`` is false."""
        ...  # pragma: no cover

    def upsert_many(
        self,
        items: Sequence[UpsertItem[C, U]],
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[R] | None]:
        """Bulk insert-or-update keyed by each :class:`UpsertItem.id`; ids must be unique.

        Result order matches ``items``.
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[False] = False,
    ) -> Awaitable[R]:
        """Apply a partial update to a document identified by ``pk``."""
        ...  # pragma: no cover

    @overload
    def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[True],
    ) -> Awaitable[tuple[R, JsonDict]]:
        """Apply a partial update to a document identified by ``pk``."""
        ...  # pragma: no cover

    @overload
    def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[False],
        return_diff: Literal[False] = False,
    ) -> Awaitable[None]:
        """Apply a partial update to a document identified by ``pk``."""
        ...  # pragma: no cover

    @overload
    def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[False],
        return_diff: Literal[True],
    ) -> Awaitable[JsonDict]:
        """Apply a partial update to a document identified by ``pk``."""
        ...  # pragma: no cover

    def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: bool = True,
        return_diff: bool = False,
    ) -> Awaitable[R | JsonDict | None | tuple[R, JsonDict]]:
        """Apply a partial update to a document identified by ``pk``."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[False] = False,
    ) -> Awaitable[Sequence[R]]:
        """Apply partial updates to multiple documents."""
        ...  # pragma: no cover

    @overload
    def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[True],
    ) -> Awaitable[Sequence[tuple[R, JsonDict]]]:
        """Apply partial updates to multiple documents."""
        ...  # pragma: no cover

    @overload
    def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[False] = False,
    ) -> Awaitable[None]:
        """Apply partial updates to multiple documents."""
        ...  # pragma: no cover

    @overload
    def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[True],
    ) -> Awaitable[Sequence[JsonDict]]:
        """Apply partial updates to multiple documents."""
        ...  # pragma: no cover

    def update_many(
        self,
        updates: Sequence[KeyedUpdate[U]],
        *,
        return_new: bool = True,
        return_diff: bool = False,
    ) -> Awaitable[
        Sequence[R] | Sequence[JsonDict] | Sequence[tuple[R, JsonDict]] | None
    ]:
        """Apply partial updates to multiple documents."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[Sequence[R]]:
        """Apply the same partial update to all documents matching *filters* in one store operation."""
        ...  # pragma: no cover

    @overload
    def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[False],
    ) -> Awaitable[int]:
        """Apply the same partial update to all documents matching *filters* in one store operation."""
        ...  # pragma: no cover

    def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[R] | int]:
        """Apply the same partial update to every document matching *filters* (bulk patch).

        **Fast store path** — does not take per-row expected revisions and does
        **not** run domain :meth:`~forze.domain.models.Document.update` (no computed-field
        side effects, soft-delete validators, or per-row OCC). Suitable for admin or
        batch flags (for example ``archived=true``). Prefer :meth:`update_matching_strict`
        when business rules must match :meth:`update` / :meth:`update_many`.

        Postgres applies a single ``UPDATE … WHERE … RETURNING``; Mongo keyset-pages
        ids and runs batched ``update_many`` with ``$inc`` on ``rev``.

        :param filters: Required filter expression (same shape as query ``find_many``).
        :param dto: Patch applied uniformly to each matching row.
        :param return_new: When ``True``, return read models for updated rows. Postgres
            hydrates from ``RETURNING`` domain rows when the coordinator can map write
            results to the read model; Mongo re-reads updated ids per chunk. When
            ``False``, return the number of rows the store reported as updated.

        With Postgres ``bookkeeping_strategy="database"``, revision bumps and timestamps
        rely on DB triggers; ``RETURNING`` may reflect pre- or post-trigger values
        depending on trigger timing (``BEFORE`` vs ``AFTER`` UPDATE).
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def update_matching_strict(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[True] = True,
        chunk_size: int | None = ...,
    ) -> Awaitable[Sequence[R]]:
        """Match :meth:`update_many` semantics via chunked :meth:`DocumentQueryPort.project_many` + :meth:`update_many`."""
        ...  # pragma: no cover

    @overload
    def update_matching_strict(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[False],
        chunk_size: int | None = ...,
    ) -> Awaitable[int]:
        """Match :meth:`update_many` semantics via chunked :meth:`DocumentQueryPort.project_many` + :meth:`update_many`."""
        ...  # pragma: no cover

    def update_matching_strict(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: bool = True,
        chunk_size: int | None = None,
    ) -> Awaitable[Sequence[R] | int]:
        """Apply the same partial update to every matching document using optimistic revisions.

        Same semantics as :meth:`update_many`: chunked keyset reads, per-row expected
        ``rev``, and domain :meth:`~forze.domain.models.Document.update` apply.
        Use instead of :meth:`update_matching` when business rules must be preserved.

        :param filters: Required filter expression.
        :param dto: Patch applied uniformly to each row in a chunk.
        :param chunk_size: Maximum rows per chunk; defaults to the adapter batch size when omitted.
        :param return_new: When ``True``, return all updated read models; when ``False``, return the count updated.
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def touch(self, pk: UUID, *, return_new: Literal[True] = True) -> Awaitable[R]:
        """Bump metadata (e.g. ``last_update_at``) for a single document."""
        ...  # pragma: no cover

    @overload
    def touch(self, pk: UUID, *, return_new: Literal[False]) -> Awaitable[None]:
        """Bump metadata (e.g. ``last_update_at``) for a single document."""
        ...  # pragma: no cover

    def touch(self, pk: UUID, *, return_new: bool = True) -> Awaitable[R | None]:
        """Bump metadata (e.g. ``last_update_at``) for a single document."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[Sequence[R]]:
        """Bump metadata for multiple documents."""
        ...  # pragma: no cover

    @overload
    def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Bump metadata for multiple documents."""
        ...  # pragma: no cover

    def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[R] | None]:
        """Bump metadata for multiple documents."""
        ...  # pragma: no cover

    # ....................... #

    def kill(self, pk: UUID) -> Awaitable[None]:
        """Hard-delete a single document without soft-delete semantics."""
        ...  # pragma: no cover

    def kill_many(self, pks: Sequence[UUID]) -> Awaitable[None]:
        """Hard-delete multiple documents."""
        ...  # pragma: no cover
