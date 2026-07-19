"""In-memory document adapter."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable, Sequence
from typing import (
    Any,
    Literal,
    cast,
    final,
    overload,
)
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.base import (
    CountlessPage,
    CursorPage,
    Page,
    page_from_limit_offset,
)
from forze.application.contracts.document import (
    DocumentCodecs,
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
    RowLockMode,
    validate_query_parameters,
)
from forze.application.contracts.domain import DomainEventDispatcherPort
from forze.application.contracts.querying import (
    AggregatesExpression,
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QuerySortExpression,
    assert_cursor_projection_includes_sort_keys,
    build_cursor_binding,
    cursor_protection_active,
    normalize_sorts_for_keyset,
    read_fields_for_model,
    resolve_effective_sorts,
    validate_query_field_types,
    validate_runtime_filter_fields,
    validate_runtime_sort_fields,
)
from forze.application.integrations.document._limits import (
    DEFAULT_MAX_STREAM_PAGES,
    assert_cursor_advanced,
    check_page_limit,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec, default_model_codec
from forze.domain.constants import ID_FIELD
from forze_mock.adapters._journal import JournalingStore
from forze_mock.adapters._mvcc import current_mvcc_tx
from forze_mock.adapters.query_params import MockQueryParamsSource
from forze_mock.query._types import (
    C,
    D,
    R,
    T,
    U,
)
from forze_mock.query.cursors import (
    _mock_keyset_window,  # pyright: ignore[reportPrivateUsage]
)
from forze_mock.query.matching import (
    _aggregate_docs,  # pyright: ignore[reportPrivateUsage]
    _match_expr,  # pyright: ignore[reportPrivateUsage]
    _project,  # pyright: ignore[reportPrivateUsage]
    _sort_docs,  # pyright: ignore[reportPrivateUsage]
)
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace

from ._document_command import MockDocumentCommandMixin


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockDocumentAdapter(  # pyright: ignore[reportIncompatibleVariableOverride]
    MockTenancyMixin,
    MockDocumentCommandMixin[R, D, C, U],
    DocumentQueryPort[R],
    DocumentCommandPort[R, D, C, U],
):
    """In-memory document adapter with filter/sort/projection support.

    Query/read operations live here; write (command) operations come from
    :class:`~forze_mock.adapters._document_command.MockDocumentCommandMixin`.
    """

    spec: DocumentSpec[R, D, C, U]
    state: MockState
    namespace: str
    read_model: type[R]
    codecs: DocumentCodecs[R, D, C, U] = attrs.field(
        default=attrs.Factory(lambda self: self.spec.resolved_codecs, takes_self=True)
    )
    """Codec bundle every (de)serialization goes through — the spec's own codecs by
    default; the module factory passes them wrapped for field encryption when the
    spec declares it, exactly as the real document factories do."""
    domain_model: type[D] | None = None
    dispatcher_provider: Callable[[], DomainEventDispatcherPort | None] = attrs.field(
        default=lambda: None
    )
    bound_params: BaseModel | None = None
    query_params_source: MockQueryParamsSource | None = None

    # ....................... #

    def with_parameters(self, params: BaseModel) -> MockDocumentAdapter[R, D, C, U]:
        # Bind the validated params onto a clone; reads then draw rows from the registered source
        # (modelling the parametrized relation) instead of stored documents.
        validate_query_parameters(self.spec, params)
        return attrs.evolve(self, bound_params=params)

    # ....................... #

    def _sealed_fields(self) -> frozenset[str]:
        """Fields the spec declares as ciphertext at rest.

        Threaded into the shared sort/keyset validators so a sealed field is refused as an
        order key from the *declaration* — the policy fires identically on every backend,
        independent of whether a cipher is wired (the mock module wires one, but a
        directly-constructed adapter may not).
        """

        return self.spec.encryption.sealed if self.spec.encryption else frozenset()

    # ....................... #

    def _require_params_bound(self) -> None:
        """Fail closed when the spec needs query parameters but none were bound."""

        if self.spec.query_params is not None and self.bound_params is None:
            raise exc.precondition(
                "This read requires query parameters; acquire the port via with_parameters(...) "
                "before reading.",
                code="query_parameters_unbound",
            )

    # ....................... #

    def _param_source_rows(self) -> list[JsonDict]:
        """Rows the registered parametrized source yields for the bound params, as mappings.

        Filtered through :meth:`_doc_visible` so a multi-tenant source can't expose other tenants'
        rows — mirroring the tenant ``WHERE`` clause the Postgres relation carries. Centralizing it
        here keeps ``get`` / ``get_many`` / ``_candidate_docs`` consistent.
        """

        if self.query_params_source is None:
            raise exc.configuration(
                f"Document {self.spec.name!r}: no mock query-parameter source registered — "
                "register one via MockQueryParamsRegistry.on().",
                code="mock.query_parameters.unprogrammed",
            )

        if self.bound_params is None:
            raise exc.precondition(
                "This read requires query parameters; acquire the port via with_parameters(...) "
                "before reading.",
                code="query_parameters_unbound",
            )

        rows = self.query_params_source(self.bound_params, self.state)

        mapped = (
            row.model_dump(mode="python") if isinstance(row, BaseModel) else dict(row)
            for row in rows
        )
        return [doc for doc in mapped if self._doc_visible(doc)]

    # ....................... #

    def _candidate_docs(self) -> list[JsonDict]:
        """Visible documents for a filter/sort/page read — from the bound query-parameter source
        when parameters are bound, otherwise from stored documents."""

        self._require_params_bound()

        if self.bound_params is not None:
            return self._param_source_rows()

        # ``_store()`` acquires ``state.lock`` itself; the comprehension has no await, so the live
        # view can't be mutated mid-iteration — no extra outer lock needed.
        return [dict(doc) for doc in self._store().values() if self._doc_visible(doc)]

    # ....................... #

    def _store(self) -> dict[UUID, JsonDict]:
        ns = partition_namespace(self.require_tenant_if_aware(), self.namespace)
        with self.state.lock:
            store = self.state.documents.get(ns)
            if not isinstance(store, JournalingStore):
                # Make the namespace store journaling so writes are atomic under a
                # transaction (coercing a plain dict left by setup/snapshot, in place).
                store = JournalingStore(store or {})
                self.state.documents[ns] = store

            # Under a snapshot/serializable transaction, reads and writes route through the
            # MVCC overlay (buffered writes + as-of-begin snapshot reads) instead of the live
            # store; read-committed (the default) uses the write-through store directly.
            mvcc = current_mvcc_tx()
            if mvcc is not None:
                return cast("dict[UUID, JsonDict]", mvcc.view(ns, store))

            return store

    # ....................... #

    def _mark_rev_guarded(self, pk: UUID) -> None:
        """Claim *pk* for a rev-guarded write on the active MVCC transaction (a no-op outside one).

        Lets read-committed surface a write-write conflict for rev-guarded writes only — a blind
        (rev-less) write does not mark, so it silently loses as read-committed permits. The claim is
        anchored at the current commit version (the version the row was read at), so a fresh
        read-then-update does not spuriously conflict. See
        :attr:`~forze_mock.adapters._mvcc.MvccTx.rev_guarded`.
        """

        mvcc = current_mvcc_tx()

        if mvcc is not None:
            ns = partition_namespace(self.require_tenant_if_aware(), self.namespace)
            mvcc.mark_rev_guarded(ns, pk, self.state.mvcc_version)

    # ....................... #

    def _mark_row_locked(self, pk: UUID) -> None:
        """Claim *pk* for a ``FOR UPDATE`` locked read (a no-op outside an MVCC transaction).

        Models Postgres ``SELECT ... FOR UPDATE``: the locked row conflicts with a concurrent writer
        the way Postgres blocks it — a read-committed transaction that locked a row a concurrent
        committer then wrote is aborted (abort-vs-block, normalized by the conformance differential),
        preventing the lost update / write skew a bare read would let through. Reuses the claim
        machinery (:attr:`~forze_mock.adapters._mvcc.MvccTx.rev_guarded`) — a lock is a claim on the
        row just like a rev-guarded write.
        """

        mvcc = current_mvcc_tx()

        if mvcc is not None:
            ns = partition_namespace(self.require_tenant_if_aware(), self.namespace)
            mvcc.mark_rev_guarded(ns, pk, self.state.mvcc_version)

    # ....................... #

    def _mark_locked_row_from(self, hit: Any) -> None:
        """Claim a ``FOR UPDATE`` lock on a predicate read's row, keyed by its id when discoverable.

        ``get`` locks by an explicit pk; ``find``/``project``/``select`` lock the returned row,
        whose id is read from the model attribute or the projected mapping. A projection that
        excludes the id (or a custom ``select`` type without one) cannot be claimed — a best-effort
        gap, not a lost update: the common id-addressable path (``get``, full-row ``find``) is exact.
        """

        raw = getattr(hit, ID_FIELD, None)

        if raw is None and isinstance(hit, dict):
            raw = hit.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                ID_FIELD
            )

        if raw is None:
            return

        self._mark_row_locked(
            raw if isinstance(raw, UUID) else UUID(str(raw))  # pyright: ignore[reportUnknownArgumentType]
        )

    # ....................... #

    def _mark_created(self, pk: UUID) -> None:
        """Record *pk* as a NEW ``create`` insert on the active MVCC transaction (no-op outside one).

        Lets commit reject a duplicate id a concurrent committer published as ``exc.conflict`` (unique
        violation), rather than silently merging. See
        :attr:`~forze_mock.adapters._mvcc.MvccTx.created`.
        """

        mvcc = current_mvcc_tx()

        if mvcc is not None:
            ns = partition_namespace(self.require_tenant_if_aware(), self.namespace)
            mvcc.mark_created(ns, pk)

    # ....................... #

    def _doc_visible(self, doc: JsonDict) -> bool:
        if not self.tenant_aware:
            return True

        tenant_id = self.require_tenant_if_aware()

        doc_tid = doc.get("tenant_id")

        return tenant_id is None if doc_tid is None else str(doc_tid) == str(tenant_id)

    # ....................... #

    def _read_codec(self) -> ModelCodec[R, Any]:
        return self.codecs.read

    def _to_read(self, doc: JsonDict) -> R:
        return self._read_codec().decode_mapping(dict(doc))

    # ....................... #

    def _require_domain_model(self) -> type[D]:
        if self.domain_model is None:
            raise exc.internal("Write support requires a domain model")
        return self.domain_model

    # ....................... #

    def _domain_codec(self) -> ModelCodec[D, Any]:
        domain = self.codecs.domain
        if domain is None:
            raise exc.internal("Domain codec is required when write is enabled")
        return domain

    def _create_codec(self) -> ModelCodec[D, Any]:
        create = self.codecs.create
        if create is None:
            raise exc.internal("Create codec is required when write is enabled")
        return create

    def _patch_codec(self) -> ModelCodec[Any, Any]:
        codecs = self.codecs
        if codecs.update is not None:
            return codecs.update
        if self.spec.write is not None:
            domain = codecs.domain
            if domain is None:
                raise exc.internal("Domain codec is required when update codec is not configured")
            return domain
        return self._read_codec()

    def _to_domain(self, doc: JsonDict) -> D:
        return self._domain_codec().decode_mapping(dict(doc))

    # ....................... #

    def _matcher(self, filters: QueryFilterExpression | None) -> Callable[[JsonDict], bool]:
        """Parse *filters* once into a reusable predicate over stored documents.

        The mock twin of the read gateway's encrypted-filter seam: an encrypting read
        codec exposes ``rewrite_filter``, which replaces the literal in an equality
        predicate on a searchable (deterministic) field with its ciphertext — so the
        predicate matches the value at rest, exactly as it does on a real backend.
        Plain codecs skip the rewrite and keep the shared evaluator's semantics.
        """

        if filters is None:
            return lambda _doc: True

        expr = QueryFilterExpressionParser.parse(filters)
        rewrite = getattr(self._read_codec(), "rewrite_filter", None)

        if rewrite is not None:
            expr = rewrite(expr)

        return lambda doc: _match_expr(doc, expr)

    # ....................... #

    def _ensure_exists(self, pk: UUID) -> JsonDict:
        store = self._store()

        if pk not in store or not self._doc_visible(store[pk]):
            raise exc.not_found(f"Document not found: {pk}")

        return store[pk]

    # ....................... #

    def _check_rev(self, current_rev: int, expected_rev: int | None) -> None:
        if expected_rev is None:
            return

        if expected_rev != current_rev:
            # Match the real adapters' rev-conflict contract exactly (the shared persistence
            # gateway raises this): a stale-rev write is a retryable PRECONDITION, not a generic
            # CONCURRENCY error — so an app's optimistic-concurrency handling behaves the same on
            # the mock as on every real backend (verified by the conformance differential).
            raise exc.precondition("Revision mismatch", code="revision_mismatch")

    # ....................... #

    def _to_read_or_projection(
        self,
        doc: JsonDict,
        return_fields: Sequence[str] | None,
    ) -> R | JsonDict:
        if return_fields is not None:
            # Mirror the real backends: a raw projection of an encrypted/searchable
            # field is decrypted before projecting (full reads already decrypt via the
            # read codec). No-op for plain codecs. Synchronous: the mock keyring cache
            # is seeded at encrypt time / via warm(), so no async pre-pass is needed.
            decrypt = getattr(self._read_codec(), "decrypt_mapping", None)
            source = decrypt(dict(doc)) if decrypt is not None else doc
            return _project(source, return_fields)
        return self._to_read(doc)

    # ....................... #

    async def get(
        self,
        pk: UUID,
        *,
        for_update: RowLockMode = False,
        skip_cache: bool = False,
    ) -> R:
        del skip_cache

        self._require_params_bound()

        if self.bound_params is not None:
            return self._to_read(self._param_doc_by_pk(pk))

        with self.state.lock:
            doc = dict(self._ensure_exists(pk))
            # ``FOR UPDATE`` (any truthy mode): claim the row so a concurrent writer conflicts, the
            # way Postgres blocks it. ``skip_locked`` degrades to this too — its disjoint-claim
            # semantics is a declared mock gap (see MECHANISM_DIVERGENCES), not a silent no-op.
            if for_update:
                self._mark_row_locked(pk)
        return self._to_read(doc)

    # ....................... #

    def _param_doc_by_pk(self, pk: UUID) -> JsonDict:
        for doc in self._param_source_rows():
            if str(doc.get(ID_FIELD)) == str(pk):
                return doc
        raise exc.not_found(f"Record not found: {pk}")

    # ....................... #

    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        skip_cache: bool = False,
    ) -> Sequence[R]:
        del skip_cache

        self._require_params_bound()

        if self.bound_params is not None:
            by_id = {str(d.get(ID_FIELD)): d for d in self._param_source_rows()}

            if missing := [pk for pk in pks if str(pk) not in by_id]:
                raise exc.not_found(f"Documents not found: {missing}")

            return [self._to_read(by_id[str(pk)]) for pk in pks]

        with self.state.lock:
            store = self._store()

            if missing := [pk for pk in pks if pk not in store]:
                raise exc.not_found(f"Documents not found: {missing}")

            docs = [dict(store[pk]) for pk in pks]

        return [self._to_read(doc) for doc in docs]

    # ....................... #

    def _validate_filter_types(
        self,
        filters: QueryFilterExpression | None,
    ) -> None:
        """Operator/field-type validation, mirroring the real gateways' ``compile_filters``.

        Keeps dev (mock) and prod (Postgres/Mongo) symmetric: a type-incompatible filter
        (e.g. ``$like`` on a number) raises the same clean ``query_operator_type_mismatch``
        precondition here instead of silently matching nothing.
        """

        if filters is None:
            return

        validate_runtime_filter_fields(
            filters,
            model=self.read_model,
            materialized=self.spec.materialized,
            lenient=self.spec.resolved_lenient_read_fields,
            # The mock stores plaintext (it is a dict, not a disk), so nothing here would stop a
            # filter on a sealed field from matching — while the same query against a real backend
            # cannot match its ciphertext. Passing the declaration keeps the *policy* identical on
            # both, so a query that fails in production fails in the test suite too.
            encrypted=self.spec.encryption.encrypted if self.spec.encryption else frozenset(),
        )
        expr = QueryFilterExpressionParser.parse(filters)
        validate_query_field_types(expr, self.read_model)

    # ....................... #

    async def find(
        self,
        filters: QueryFilterExpression,
        *,
        for_update: RowLockMode = False,
    ) -> R | None:
        page = await self.find_many(
            filters=filters,
            pagination={"limit": 1},
        )

        if not page.hits:
            return None

        hit = page.hits[0]

        if for_update:
            self._mark_locked_row_from(hit)

        return hit

    # ....................... #

    async def project(
        self,
        filters: QueryFilterExpression,
        fields: Sequence[str],
        *,
        for_update: RowLockMode = False,
    ) -> JsonDict | None:
        page = await self.project_many(
            tuple(fields),
            filters=filters,
            pagination={"limit": 1},
        )

        if not page.hits:
            return None

        hit = page.hits[0]

        if for_update:
            self._mark_locked_row_from(hit)

        return hit

    # ....................... #

    async def select(
        self,
        filters: QueryFilterExpression,
        return_type: type[T],
        *,
        for_update: RowLockMode = False,
    ) -> T | None:
        page = await self.select_many(
            return_type,
            filters=filters,
            pagination={"limit": 1},
        )

        if not page.hits:
            return None

        hit = page.hits[0]

        if for_update:
            self._mark_locked_row_from(hit)

        return hit

    # ....................... #

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: None,
        return_type: None,
        return_fields: None,
    ) -> CountlessPage[R]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: None,
        return_type: None,
        return_fields: None,
    ) -> Page[R]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: None,
        return_type: None,
        return_fields: Sequence[str],
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: None,
        return_type: None,
        return_fields: Sequence[str],
    ) -> Page[JsonDict]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: None,
        return_type: type[T],
        return_fields: None,
    ) -> CountlessPage[T]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: None,
        return_type: type[T],
        return_fields: None,
    ) -> Page[T]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: AggregatesExpression,
        return_type: None,
        return_fields: None,
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: AggregatesExpression,
        return_type: None,
        return_fields: None,
    ) -> Page[JsonDict]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: AggregatesExpression,
        return_type: type[T],
        return_fields: None,
    ) -> CountlessPage[T]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: AggregatesExpression,
        return_type: type[T],
        return_fields: None,
    ) -> Page[T]: ...

    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: bool,
        aggregates: AggregatesExpression | None,
        return_type: type[Any] | None,
        return_fields: Sequence[str] | None,
    ) -> Any:
        if aggregates is not None and return_fields is not None:
            raise exc.internal("Aggregates cannot be combined with return_fields")

        self._validate_filter_types(filters)

        docs = self._candidate_docs()

        match = self._matcher(filters)
        filtered = [doc for doc in docs if match(doc)]

        pagination = pagination or {}
        limit_raw = pagination.get("limit")
        # Normalize to ints up front (callers may pass string limit/offset) so the slicing
        # arithmetic in ``_page_window`` is always numeric.
        limit = int(limit_raw) if limit_raw is not None else None
        offset = int(pagination.get("offset") or 0)

        def _page_window(ordered: list[Any]) -> list[Any]:
            # Slice to the requested page *before* projecting/decoding, so only the page's rows
            # are materialized — matching the real adapters' late materialization (the DB
            # applies OFFSET/LIMIT before hydration) rather than decoding the whole match set.
            return ordered[offset : offset + limit] if limit is not None else ordered[offset:]

        rows: list[Any]

        if aggregates is not None:
            aggregate_rows = _aggregate_docs(filtered, aggregates)
            total = len(aggregate_rows)
            page_rows = _page_window(_sort_docs(aggregate_rows, sorts))
            rows = (
                default_model_codec(return_type).decode_mapping_many(page_rows)
                if return_type is not None
                else page_rows
            )
        else:
            validate_runtime_sort_fields(
                sorts,
                model=self.read_model,
                backend="mock",
                materialized=self.spec.materialized,
                lenient=self.spec.resolved_lenient_read_fields,
                sealed=self._sealed_fields(),
            )
            total = len(filtered)
            page_docs = _page_window(_sort_docs(filtered, sorts))
            if return_type is not None:
                dict_rows: list[dict[str, Any]] = []

                for doc in page_docs:
                    row = self._to_read_or_projection(doc, return_fields)
                    if isinstance(row, BaseModel):
                        dict_rows.append(row.model_dump(mode="python"))
                    else:
                        dict_rows.append(dict(row))

                rows = default_model_codec(return_type).decode_mapping_many(dict_rows)
            else:
                rows = [self._to_read_or_projection(doc, return_fields) for doc in page_docs]

        if return_count:
            return page_from_limit_offset(
                cast(Any, rows),
                pagination,
                total=total,
            )
        return page_from_limit_offset(cast(Any, rows), pagination, total=None)

    # ....................... #

    async def find_many(
        self,
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[R]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=None,
            return_type=None,
            return_fields=None,
        )

    # ....................... #

    async def project_many(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[JsonDict]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=None,
            return_type=None,
            return_fields=tuple(fields),
        )

    # ....................... #

    async def select_many(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[T]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=None,
            return_type=return_type,
            return_fields=None,
        )

    # ....................... #

    async def find_page(
        self,
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[R]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=None,
            return_type=None,
            return_fields=None,
        )

    # ....................... #

    async def project_page(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[JsonDict]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=None,
            return_type=None,
            return_fields=tuple(fields),
        )

    # ....................... #

    async def select_page(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[T]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=None,
            return_type=return_type,
            return_fields=None,
        )

    # ....................... #

    async def aggregate_many(
        self,
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[JsonDict]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=aggregates,
            return_type=None,
            return_fields=None,
        )

    # ....................... #

    async def aggregate_page(
        self,
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[JsonDict]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=aggregates,
            return_type=None,
            return_fields=None,
        )

    # ....................... #

    async def select_many_aggregated(
        self,
        return_type: type[T],
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[T]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=aggregates,
            return_type=return_type,
            return_fields=None,
        )

    # ....................... #

    async def select_page_aggregated(
        self,
        return_type: type[T],
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[T]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=aggregates,
            return_type=return_type,
            return_fields=None,
        )

    # ....................... #

    async def find_cursor(
        self,
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CursorPage[R]:
        return await self._mock_cursor_page(
            filters=filters,
            cursor=cursor,
            sorts=sorts,
            return_fields=None,
        )

    # ....................... #

    async def project_cursor(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CursorPage[JsonDict]:
        return await self._mock_cursor_page(
            filters=filters,
            cursor=cursor,
            sorts=sorts,
            return_fields=tuple(fields),
        )

    # ....................... #

    async def select_cursor(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CursorPage[T]:
        page = await self.find_cursor(filters=filters, cursor=cursor, sorts=sorts)
        # Python-mode dump keeps UUID/datetime objects intact, matching the
        # offset select path (audit: cursor vs offset value-type parity).
        return CursorPage(
            hits=[
                default_model_codec(return_type).decode_mapping(hit.model_dump(mode="python"))
                for hit in page.hits
            ],
            next_cursor=page.next_cursor,
            prev_cursor=page.prev_cursor,
            has_more=page.has_more,
        )

    # ....................... #

    async def find_stream(
        self,
        filters: QueryFilterExpression | None = None,
        *,
        sorts: QuerySortExpression | None = None,
        chunk_size: int = 500,
        max_stream_pages: int | None = DEFAULT_MAX_STREAM_PAGES,
    ) -> AsyncGenerator[Sequence[R]]:
        cursor: CursorPaginationExpression | None = {"limit": chunk_size}
        page_num = 0
        prev_cursor: str | None = None

        while True:
            check_page_limit(
                pages=page_num,
                max_pages=max_stream_pages,
                label="Mock find_stream",
            )
            page = await self.find_cursor(filters=filters, cursor=cursor, sorts=sorts)

            if not page.hits:
                break

            yield page.hits

            if not page.has_more or page.next_cursor is None:
                break

            assert_cursor_advanced(
                prev_cursor=prev_cursor,
                next_cursor=page.next_cursor,
            )
            prev_cursor = page.next_cursor
            cursor = {"limit": chunk_size, "after": page.next_cursor}
            page_num += 1

    # ....................... #

    async def project_stream(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,
        *,
        sorts: QuerySortExpression | None = None,
        chunk_size: int = 500,
        max_stream_pages: int | None = DEFAULT_MAX_STREAM_PAGES,
    ) -> AsyncGenerator[Sequence[JsonDict]]:
        cursor: CursorPaginationExpression | None = {"limit": chunk_size}
        page_num = 0
        prev_cursor: str | None = None

        while True:
            check_page_limit(
                pages=page_num,
                max_pages=max_stream_pages,
                label="Mock project_stream",
            )
            page = await self.project_cursor(
                fields,
                filters=filters,
                cursor=cursor,
                sorts=sorts,
            )

            if not page.hits:
                break

            yield page.hits

            if not page.has_more or page.next_cursor is None:
                break

            assert_cursor_advanced(
                prev_cursor=prev_cursor,
                next_cursor=page.next_cursor,
            )
            prev_cursor = page.next_cursor
            cursor = {"limit": chunk_size, "after": page.next_cursor}
            page_num += 1

    # ....................... #

    async def select_stream(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,
        *,
        sorts: QuerySortExpression | None = None,
        chunk_size: int = 500,
        max_stream_pages: int | None = DEFAULT_MAX_STREAM_PAGES,
    ) -> AsyncGenerator[Sequence[T]]:
        cursor: CursorPaginationExpression | None = {"limit": chunk_size}
        page_num = 0
        prev_cursor: str | None = None

        while True:
            check_page_limit(
                pages=page_num,
                max_pages=max_stream_pages,
                label="Mock select_stream",
            )
            page = await self.select_cursor(
                return_type,
                filters=filters,
                cursor=cursor,
                sorts=sorts,
            )

            if not page.hits:
                break

            yield page.hits

            if not page.has_more or page.next_cursor is None:
                break

            assert_cursor_advanced(
                prev_cursor=prev_cursor,
                next_cursor=page.next_cursor,
            )
            prev_cursor = page.next_cursor
            cursor = {"limit": chunk_size, "after": page.next_cursor}
            page_num += 1

    # ....................... #

    @overload
    async def _mock_cursor_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        cursor: CursorPaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_fields: None,
    ) -> CursorPage[R]: ...

    @overload
    async def _mock_cursor_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        cursor: CursorPaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_fields: Sequence[str],
    ) -> CursorPage[JsonDict]: ...

    async def _mock_cursor_page(
        self,
        *,
        filters: QueryFilterExpression | None,
        cursor: CursorPaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_fields: Sequence[str] | None,
    ) -> CursorPage[R] | CursorPage[JsonDict]:
        # Keyset pagination on the shared cursor-token machinery, mirroring
        # the real document gateways: resolve the effective sort (+ id
        # tie-breaker), seek past the token's sort values, and mint the next
        # cursor from the last returned row.
        self._validate_filter_types(filters)

        read_fields = read_fields_for_model(self.read_model) | self.spec.materialized
        effective = resolve_effective_sorts(
            sorts=sorts,
            default_sort=self.spec.default_sort,
            read_fields=read_fields,
            spec_name=self.spec.name,
            model=self.read_model,
        )
        normalized = normalize_sorts_for_keyset(
            effective, read_fields=read_fields, model=self.read_model, sealed=self._sealed_fields()
        )
        sort_keys = [k for k, _, _ in normalized]
        directions = [d for _, d, _ in normalized]
        nulls = [n for _, _, n in normalized]

        assert_cursor_projection_includes_sort_keys(
            return_fields=return_fields,
            sort_keys=sort_keys,
        )

        docs = self._candidate_docs()

        binding = (
            build_cursor_binding(
                # Spec-less, mirroring the real document read path (``document_cursor_binding``):
                # the generic gateway has no spec, so a document cursor binds on tenant + filter
                # only. Using a spec name here would diverge from the backends the mock models.
                spec_name=None,
                tenant_id=self.require_tenant_if_aware(),
                filter_expr=(QueryFilterExpressionParser.parse(filters) if filters else None),
            )
            if cursor_protection_active()
            else None
        )

        match = self._matcher(filters)
        filtered = [doc for doc in docs if match(doc)]
        page_docs, has_more, next_c, prev_c = _mock_keyset_window(
            filtered,
            cursor=cursor,
            sort_keys=sort_keys,
            directions=directions,
            nulls=nulls,
            binding=binding,
        )
        if return_fields is not None:
            out_raw = [self._to_read_or_projection(doc, return_fields) for doc in page_docs]
            return CursorPage(
                hits=cast(list[JsonDict], out_raw),
                next_cursor=next_c,
                prev_cursor=prev_c,
                has_more=has_more,
            )
        out_typed = [self._to_read_or_projection(doc, None) for doc in page_docs]
        return CursorPage(
            hits=cast(list[R], out_typed),
            next_cursor=next_c,
            prev_cursor=prev_c,
            has_more=has_more,
        )

    # ....................... #

    async def count(self, filters: QueryFilterExpression | None = None) -> int:
        self._validate_filter_types(filters)

        docs = self._candidate_docs()
        match = self._matcher(filters)
        return sum(bool(match(doc)) for doc in docs)
