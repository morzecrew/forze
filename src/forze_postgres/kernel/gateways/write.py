"""Write gateway for creating, updating, soft-deleting, and hard-deleting Postgres documents."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections import defaultdict
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager, nullcontext
from functools import partial
from typing import Any, LiteralString, cast, final, get_args
from uuid import UUID

import attrs
from psycopg import sql
from pydantic.fields import FieldInfo

from forze.application.contracts.document import domains_from_create_payloads
from forze.application.contracts.guarantees import SerializedBy
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.resilience import ResilienceExecutorPort
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.application.execution.resilience import default_resilience_executor
from forze.application.integrations.persistence import (
    DocumentWriteCodecMixin,
    HistoryOccMixin,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, OnceCell, advisory_lock_key, utcnow
from forze.base.serialization import ModelCodec
from forze.domain.constants import ID_FIELD, LAST_UPDATE_AT_FIELD, REV_FIELD
from forze.domain.models import BaseDTO, Document
from forze_postgres.kernel.catalog.introspect import PostgresColumnTypes, PostgresType
from forze_postgres.kernel.catalog.introspect.utils import normalize_pg_type, strip_type_modifier
from forze_postgres.kernel.client import gather_db_work
from forze_postgres.kernel.sql.conflict_target import resolve_write_conflict_target

from ._occ import postgres_occ_retry
from .base import PostgresGateway
from .history import PostgresHistoryGateway
from .read import PostgresReadGateway
from .types import PostgresBookkeepingStrategy

# ----------------------- #


def _pg_cast_type_sql(pg: PostgresType) -> sql.Composable:
    """Return the ``CAST`` target type for a column (names come from introspection only)."""

    base = cast(LiteralString, pg.base)  # type: ignore[redundant-cast]

    if pg.is_array:
        return sql.SQL(cast(LiteralString, pg.base + "[]"))  # type: ignore[redundant-cast]

    return sql.SQL(base)


def _values_placeholder_for_patch_group(
    *,
    column: str,
    expected_rev_alias: str,
    column_types: PostgresColumnTypes,
) -> sql.Composable:
    """Placeholder for one ``VALUES`` cell, typed so all-``NULL`` columns are not inferred as ``text``."""

    ph = sql.Placeholder()
    if column == ID_FIELD:
        pg_t = column_types.get(ID_FIELD)
    elif column == expected_rev_alias:
        pg_t = column_types.get(REV_FIELD)
    else:
        pg_t = column_types.get(column)

    if pg_t is None:
        return ph

    return sql.SQL("CAST({} AS {})").format(ph, _pg_cast_type_sql(pg_t))


_UNNEST_BASES: frozenset[str] = frozenset(
    {
        "uuid",
        "text",
        "varchar",
        "char",
        "bpchar",
        "citext",
        "bool",
        "int2",
        "int4",
        "int8",
        "float4",
        "float8",
        "numeric",
        "date",
        "timestamp",
        "timestamptz",
        "time",
        "timetz",
        "interval",
        "json",
        "jsonb",
        "bytea",
        "inet",
        "cidr",
        "macaddr",
    }
)
"""Built-in scalar types an ``unnest`` array cast can name exactly as introspected."""

_NO_EQUALITY: frozenset[str] = frozenset(
    {
        "xml",
        "point",
        "line",
        "lseg",
        "box",
        "path",
        "polygon",
        "circle",
        "citext",
        "char",
        "bpchar",
    }
)
"""Types ``IS DISTINCT FROM`` cannot compare as the domain does: no equality at all, or one
that calls two different values equal (``citext`` ignores case, ``char`` trailing blanks).
``json`` compares once cast to ``jsonb``."""


def _scalar_base(pg_t: PostgresType) -> str:
    return normalize_pg_type(strip_type_modifier(pg_t.base))


def _comparable(pg_t: PostgresType | None) -> bool:
    """Whether a patched column can be compared with its new value in SQL."""

    if pg_t is None:
        return True

    base = _scalar_base(pg_t)

    return base not in _NO_EQUALITY and not (base == "json" and pg_t.is_array)


def _compared(alias: str, column: str, pg_t: PostgresType | None) -> sql.Composable:
    """*alias*.*column* as ``IS DISTINCT FROM`` compares it: ``json`` as ``jsonb``."""

    ref = sql.SQL("{}.{}").format(sql.Identifier(alias), sql.Identifier(column))

    if pg_t is not None and not pg_t.is_array and _scalar_base(pg_t) == "json":
        return sql.SQL("{}::jsonb").format(ref)

    return ref


def _row_source(
    columns: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
    column_types: PostgresColumnTypes,
    *,
    typed_values: bool = False,
) -> tuple[sql.Composable, list[Any]]:
    """*rows* as a row source for ``INSERT … SELECT`` or ``FROM ( … )``, with its parameters.

    ``SELECT * FROM unnest(%s::type[], …)`` when every column has a built-in scalar type: one
    array per column, so the statement stays a few bytes and is parsed once whatever the row
    count. Otherwise (an array, which ``unnest`` would flatten, a user-defined or untyped
    column) the rows are spelled out as ``VALUES``, each cell cast to its column type when
    *typed_values*, as the domain path's batched update casts them.
    """

    casts: list[sql.Composable] = []

    for column in columns:
        pg_t = column_types.get(column)

        if pg_t is None or pg_t.is_array or _scalar_base(pg_t) not in _UNNEST_BASES:
            break

        casts.append(sql.SQL("{}::{}[]").format(sql.Placeholder(), _pg_cast_type_sql(pg_t)))

    else:
        return (
            sql.SQL("SELECT * FROM unnest({})").format(sql.SQL(", ").join(casts)),
            [[row[column] for row in rows] for column in columns],
        )

    if typed_values:
        cells = [
            _values_placeholder_for_patch_group(
                column=c, expected_rev_alias="", column_types=column_types
            )
            for c in columns
        ]

    else:
        cells = [sql.Placeholder() for _ in columns]

    row_template = sql.SQL("(") + sql.SQL(", ").join(cells) + sql.SQL(")")

    return (
        sql.SQL("VALUES {}").format(sql.SQL(", ").join([row_template] * len(rows))),
        [row[column] for row in rows for column in columns],
    )


def _null_is_null(field: FieldInfo) -> bool:
    """Whether an explicit ``None`` stores ``NULL``. A domain update drops a nulled key and
    revalidates, so the field takes its default, or is refused when it has none."""

    # A factory, which may read the other fields, is the domain's to call.
    return not field.is_required() and field.default_factory is None and field.default is None


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresWriteGateway[D: Document, C: BaseDTO, U: BaseDTO](
    DocumentWriteCodecMixin[D],
    HistoryOccMixin[D],
    PostgresGateway[D],
):
    """Write gateway for document mutations with optimistic concurrency control.

    Requires a companion :class:`PostgresReadGateway` sharing the same client.
    Optionally writes revision history via :class:`PostgresHistoryGateway`.
    All mutating operations are wrapped with the ``occ`` resilience policy via
    the transaction-aware :func:`~forze_postgres.kernel.gateways._occ.postgres_occ_retry`.
    """

    read_gw: PostgresReadGateway[D]
    """Read gateway for the same document type."""

    resilience: ResilienceExecutorPort = attrs.field(
        factory=default_resilience_executor,
        eq=False,
        repr=False,
    )
    """Resilience executor backing optimistic-concurrency retries."""

    create_cmd_type: type[C]
    """Pydantic model for creation payloads."""

    update_cmd_type: type[U] | None = attrs.field(default=None)
    """Pydantic model for update payloads."""

    create_codec: ModelCodec[D, Any] = attrs.field(kw_only=True, eq=False, repr=False)
    """Codec for create commands."""

    update_codec: ModelCodec[U, Any] | None = attrs.field(kw_only=True, eq=False, repr=False)
    """Codec for update commands when :attr:`update_cmd_type` is set; else ``None``."""

    history_gw: PostgresHistoryGateway[D] | None = attrs.field(default=None)  # type: ignore[override]
    """Optional history gateway for revision snapshots."""

    strategy: PostgresBookkeepingStrategy
    """Bookkeeping strategy."""

    conflict_target: tuple[str, ...] | None = attrs.field(default=None)
    """``ON CONFLICT`` columns for :meth:`ensure` / :meth:`upsert`; ``None`` infers PRIMARY KEY."""

    update_matching_max_rows: int | None = attrs.field(default=1_000_000)
    """Cap on how many rows a single :meth:`update_matching` may touch.

    The call snapshots the matching primary keys into memory (to keep the matched set
    stable across the chunked update), so an over-broad filter would otherwise pull an
    unbounded number of keys in. When a filter matches more than this, the call fails
    with ``precondition`` instead — narrow the filter or paginate. ``None`` disables the
    cap (accept the unbounded snapshot). Default one million."""

    serialized_by: tuple[SerializedBy, ...] = attrs.field(default=())
    """The spec's ``SerializedBy`` declarations: whose writes this gateway keeps apart."""

    serialization_scope: str = attrs.field(default="")
    """The spec's name, which every lock key carries so two aggregates never contend."""

    _conflict_target_cell: OnceCell[tuple[str, ...]] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if self.serialized_by and not self.serialization_scope:
            # Every lock key carries the scope; without one, two aggregates keyed on the same
            # value would wait on each other for no reason the declaration states.
            raise exc.configuration(
                "Serializing writes needs the spec's name as the lock scope; got none."
            )

        if self.client is not self.read_gw.client:
            raise exc.internal(
                "Client mismatch. Write gateway and nested read gateway must use the same client."
            )

        if self.tenant_aware != self.read_gw.tenant_aware:
            raise exc.internal(
                "Tenant awareness mismatch. Write gateway and nested read gateway must have the same tenant awareness."
            )

        if self.history_gw is not None:
            if self.client is not self.history_gw.client:
                raise exc.internal(
                    "Client mismatch. Write gateway and nested history gateway must use the same client."
                )

            if self.tenant_aware != self.history_gw.tenant_aware:
                raise exc.internal(
                    "Tenant awareness mismatch. Write gateway and nested history gateway must have the same tenant awareness."
                )

        if self.strategy not in get_args(PostgresBookkeepingStrategy):
            raise exc.internal(f"Invalid bookkeeping strategy: {self.strategy}")

    # ....................... #

    @asynccontextmanager
    async def _write_tx(self) -> AsyncGenerator[None]:
        """Use an outer transaction for multi-step writes when the caller has not opened one."""

        if self.client.is_in_transaction():
            yield
            return

        async with self.client.transaction():
            yield

    # ....................... #

    async def _serialize(self, touched: Callable[[], Awaitable[Sequence[Any]]]) -> None:
        """Hold the per-owner advisory lock for every owner a write touches, until it commits.

        Called inside the write's transaction: ``pg_advisory_xact_lock`` is released by commit or
        rollback, so no path can forget it, and a write outside a caller's transaction runs in
        the one :meth:`_write_tx` opens — held until that write lands. Keys are taken sorted, so
        two calls writing the same owners queue rather than deadlock; a cycle across calls is
        Postgres's to detect, and it refuses one side as ``concurrency``.

        *touched* names the rows the write touches — what it inserts, what it changes and where
        the change moves them — and is asked again after every wait: a row can change owner, or
        be committed into a filter, while this call waits, and the owner it has then was never in
        the first answer. Advisory locks are re-entrant within a session, so a method that
        delegates to another re-takes its owners without waiting.
        """

        if not self.serialized_by:
            return

        stmt = sql.SQL("SELECT pg_advisory_xact_lock({})").format(sql.Placeholder())
        held: set[int] = set()

        while wanted := sorted(self._owner_keys(await touched()) - held):
            for key in wanted:
                await self.client.execute(stmt, [key])
                held.add(key)

    async def _serialize_rows(self, rows: Sequence[Any]) -> None:
        """:meth:`_serialize` for rows the write already has — the ones it inserts."""

        async def fixed() -> Sequence[Any]:
            return rows

        await self._serialize(fixed)

    def _owner_keys(self, rows: Sequence[Any]) -> set[int]:
        """The lock keys *rows* contend on — derived as the in-memory store derives them."""

        tenant = self.require_tenant_if_aware() if self.tenant_aware else None
        keys: set[int] = set()

        for guarantee in self.serialized_by:
            for row in rows:
                values = [
                    row.get(field) if isinstance(row, Mapping) else getattr(row, field, None)
                    for field in guarantee.key
                ]
                keys.add(
                    advisory_lock_key(self.serialization_scope, tenant, *guarantee.key, *values)
                )

        return keys

    def _moved(self, current: D, patch: JsonDict | None) -> D:
        """Where *patch* moves *current* — the destination a write holds beside its origin."""

        if patch is None:
            return current

        moved, _ = current.update(patch, materialized=self.read_codec.materialized)

        return moved

    # ....................... #

    def _require_update_cmd(self) -> None:
        if self.update_cmd_type is None:
            raise exc.internal("Update command type is not supported for this model")

    # ....................... #

    def _from_create_dto(self, payload: C, id: UUID | None = None) -> D:
        model = self.create_codec.transform(payload)

        if id is not None:
            model = model.model_copy(update={ID_FIELD: id}, deep=True)

        return model

    # ....................... #

    def _patch_codec(self) -> ModelCodec[Any, Any]:
        if self.update_codec is not None:
            return self.update_codec

        if self.update_cmd_type is not None:
            raise exc.configuration("Update codec is required when update commands are supported")

        return self.read_codec

    # ....................... #

    def _ident_rev(self) -> sql.Composable:
        return sql.Identifier(REV_FIELD)

    # ....................... #

    def _where_pk_rev(self) -> sql.Composable:
        return sql.SQL("{} = {} AND {} = {}").format(
            self.ident_pk(),
            sql.Placeholder(),
            self._ident_rev(),
            sql.Placeholder(),
        )

    # ....................... #

    async def _resolved_conflict_target(self) -> tuple[str, ...]:
        async def _factory() -> tuple[str, ...]:
            return await resolve_write_conflict_target(
                self.introspector,
                schema=(await self._qname()).schema,
                relation=(await self._qname()).name,
                configured=self.conflict_target,
            )

        # Conflict columns are the table's PK/unique columns — identical across
        # tenant schemas (tenant-independent), so always memoized.
        return await self._conflict_target_cell.resolve(_factory)

    async def _ident_conflict_target(self) -> sql.Composable:
        cols = await self._resolved_conflict_target()

        return sql.SQL(", ").join(sql.Identifier(c) for c in cols)

    # ....................... #

    @postgres_occ_retry
    async def create(self, payload: C, *, id: UUID | None = None) -> D:
        async with self._write_tx():
            model = self._from_create_dto(payload, id)
            await self._serialize_rows([model])
            insert_data_raw = await self._encode_domain_one(model)
            insert_data = await self.adapt_payload_for_write(insert_data_raw, create=True)

            cols = [sql.Identifier(k) for k in insert_data]
            vals = [sql.Placeholder() for _ in insert_data]
            params = list(insert_data.values())

            stmt = sql.SQL("INSERT INTO {table} ({cols}) VALUES ({vals}) RETURNING {ret}").format(
                table=(await self._qname()).ident(),
                cols=sql.SQL(", ").join(cols),
                vals=sql.SQL(", ").join(vals),
                ret=self.return_clause(),
            )

            row = await self.client.fetch_one(stmt, params, row_factory="dict", commit=False)

            if row is None:
                raise exc.concurrency(
                    "Failed to create a record",
                    code="create_failed",
                )

            res = self._decode_row(row)
            await self._write_history(res)

            return res

    # ....................... #

    @postgres_occ_retry
    async def create_many(
        self,
        payloads: Sequence[C],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        if not payloads:
            return []

        async with self._write_tx():
            keys: list[str] | None = None
            col_idents: list[sql.Composable] | None = None
            row_template: sql.Composable | None = None
            payload_batches: list[list[JsonDict]] = []
            created: list[D] = []

            async def _insert_batch(batch: Sequence[JsonDict]) -> list[JsonDict]:
                nonlocal keys, col_idents, row_template

                if keys is None or col_idents is None or row_template is None:
                    raise exc.internal("insert_batch: missing required state")

                value_parts = [row_template] * len(batch)
                params = [b[k] for b in batch for k in keys]

                stmt = sql.SQL("INSERT INTO {table} ({cols}) VALUES {vals} RETURNING {ret}").format(
                    table=(await self._qname()).ident(),
                    cols=sql.SQL(", ").join(col_idents),
                    vals=sql.SQL(", ").join(value_parts),
                    ret=self.return_clause(),
                )

                rows = await self.client.fetch_all(
                    stmt,
                    params,
                    row_factory="dict",
                    commit=False,
                )

                if len(rows) != len(batch):
                    raise exc.concurrency(
                        "Failed to create records (mismatch in number of rows)",
                        code="create_many_mismatch",
                    )

                return rows

            for offset in range(0, len(payloads), batch_size):
                payload_batch = payloads[offset : offset + batch_size]
                models = domains_from_create_payloads(self.create_codec, payload_batch)
                created.extend(models)
                insert_data_raw = await self._encode_domain_many(models)
                insert_data = await self.adapt_many_payload_for_write(
                    insert_data_raw,
                    create=True,
                )

                if keys is None:
                    keys = list(insert_data[0].keys())
                    col_idents = [sql.Identifier(k) for k in keys]
                    row_template = (
                        sql.SQL("(")
                        + sql.SQL(", ").join(sql.Placeholder() for _ in keys)
                        + sql.SQL(")")
                    )

                elif list(insert_data[0].keys()) != keys:
                    raise exc.internal(
                        "create_many: adapted payload keys differ between batches",
                    )

                payload_batches.append(list(insert_data))

            await self._serialize_rows(created)

            batch_results = await gather_db_work(
                self.client,
                [partial(_insert_batch, b) for b in payload_batches],
            )

            result: list[D] = []
            for rows in batch_results:
                result.extend(self._decode_rows(rows))

            if len(result) != len(payloads):
                raise exc.internal("Failed to create all records")

            await self._write_history(*result)

            return result

    # ....................... #

    @postgres_occ_retry
    async def ensure(self, id: UUID, payload: C) -> D:
        """Insert a row at *id* when absent; otherwise return the existing row.

        Conflict is resolved on the primary key column (``id``) without updating
        existing rows.
        """

        async with self._write_tx():
            model = self._from_create_dto(payload, id)
            await self._serialize_rows([model])
            insert_data_raw = await self._encode_domain_one(model)
            insert_data = await self.adapt_payload_for_write(insert_data_raw, create=True)

            cols = [sql.Identifier(k) for k in insert_data]
            vals = [sql.Placeholder() for _ in insert_data]
            params = list(insert_data.values())

            conflict = await self._ident_conflict_target()
            stmt = sql.SQL(
                "INSERT INTO {table} ({cols}) VALUES ({vals}) "
                "ON CONFLICT ({conflict}) DO NOTHING "
                "RETURNING {ret}"
            ).format(
                table=(await self._qname()).ident(),
                cols=sql.SQL(", ").join(cols),
                vals=sql.SQL(", ").join(vals),
                conflict=conflict,
                ret=self.return_clause(),
            )

            row = await self.client.fetch_one(stmt, params, row_factory="dict", commit=False)

            if row is not None:
                res = self._decode_row(row)
                await self._write_history(res)
                return res

            existing = await self._fetch_domain_by_pk(model.id)
            return existing

    # ....................... #

    @postgres_occ_retry
    async def ensure_many(
        self,
        ids: Sequence[UUID],
        payloads: Sequence[C],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Bulk insert rows when their primary keys are absent; return full rows in order.

        Each id must appear at most once. Conflicts on the primary key column do not
        update existing rows. History is written only for newly inserted documents.
        """

        if not payloads:
            return []

        async with self._write_tx():
            keys: list[str] | None = None
            col_idents: list[sql.Composable] | None = None
            row_template: sql.Composable | None = None

            def _pk_from_row(r: JsonDict) -> UUID:
                v = r[ID_FIELD]

                if isinstance(v, UUID):
                    return v

                return UUID(str(v))

            async def _ensure_batch(
                batch: Sequence[JsonDict],
                model_batch: Sequence[D],
            ) -> list[D]:
                nonlocal keys, col_idents, row_template

                if keys is None or col_idents is None or row_template is None:
                    raise exc.internal("ensure_batch: missing required state")

                value_parts = [row_template] * len(batch)
                params = [b[k] for b in batch for k in keys]

                conflict = await self._ident_conflict_target()
                stmt = sql.SQL(
                    "INSERT INTO {table} ({cols}) VALUES {vals} "
                    "ON CONFLICT ({conflict}) DO NOTHING "
                    "RETURNING {ret}"
                ).format(
                    table=(await self._qname()).ident(),
                    cols=sql.SQL(", ").join(col_idents),
                    vals=sql.SQL(", ").join(value_parts),
                    conflict=conflict,
                    ret=self.return_clause(),
                )

                rows = await self.client.fetch_all(
                    stmt,
                    params,
                    row_factory="dict",
                    commit=False,
                )

                by_returned: dict[UUID, JsonDict] = {_pk_from_row(r): r for r in rows}
                need = [m.id for m in model_batch if m.id not in by_returned]

                if need:
                    fetched = await self._fetch_domains_by_pks(need)
                    by_existing = {d.id: d for d in fetched}

                else:
                    by_existing = {}

                ordered: list[D] = []
                inserted: list[D] = []

                for m in model_batch:
                    rj = by_returned.get(m.id)

                    if rj is not None:
                        dom = self._decode_row(rj)
                        inserted.append(dom)
                        ordered.append(dom)

                    else:
                        ex = by_existing.get(m.id)

                        if ex is None:
                            raise exc.not_found(
                                f"Record not found after ensure_many conflict: {m.id!s}",
                            )

                        ordered.append(ex)

                if inserted:
                    await self._write_history(*inserted)

                return ordered

            if self.serialized_by:
                await self._serialize_rows(
                    domains_from_create_payloads(self.create_codec, payloads, ids)
                )

            out: list[D] = []

            for offset in range(0, len(payloads), batch_size):
                id_batch = ids[offset : offset + batch_size]
                payload_batch = payloads[offset : offset + batch_size]
                models = domains_from_create_payloads(self.create_codec, payload_batch, id_batch)
                insert_data_raw = await self._encode_domain_many(models)
                insert_data = await self.adapt_many_payload_for_write(
                    insert_data_raw,
                    create=True,
                )

                if keys is None:
                    keys = list(insert_data[0].keys())
                    col_idents = [sql.Identifier(k) for k in keys]
                    row_template = (
                        sql.SQL("(")
                        + sql.SQL(", ").join(sql.Placeholder() for _ in keys)
                        + sql.SQL(")")
                    )

                elif list(insert_data[0].keys()) != keys:
                    raise exc.internal(
                        "ensure_many: adapted payload keys differ between batches",
                    )

                out.extend(await _ensure_batch(insert_data, models))

            if len(out) != len(payloads):
                raise exc.internal("ensure_many result length does not match input")

            return out

    # ....................... #

    @postgres_occ_retry
    async def upsert(self, id: UUID, create: C, update: U) -> D:
        """Insert *create* at *id* when free; otherwise apply ``update`` like :meth:`update`.

        ``database`` and ``application`` strategies both use the same pattern:
        attempt ``INSERT ... ON CONFLICT DO NOTHING``; on conflict, load the row
        and delegate to :meth:`update` with the current revision.
        """

        self._require_update_cmd()

        async with self._write_tx():
            model = self._from_create_dto(create, id)

            # The create's owner and the stored row's; the update arm, if it runs, takes the
            # owner it moves the row to itself.
            async def touched() -> Sequence[Any]:
                return [model, *await self._fetch_domains_by_pks([model.id], missing_ok=True)]

            await self._serialize(touched)
            insert_data_raw = await self._encode_domain_one(model)
            insert_data = await self.adapt_payload_for_write(insert_data_raw, create=True)

            cols = [sql.Identifier(k) for k in insert_data]
            vals = [sql.Placeholder() for _ in insert_data]
            params = list(insert_data.values())

            conflict = await self._ident_conflict_target()
            stmt = sql.SQL(
                "INSERT INTO {table} ({cols}) VALUES ({vals}) "
                "ON CONFLICT ({conflict}) DO NOTHING "
                "RETURNING {ret}"
            ).format(
                table=(await self._qname()).ident(),
                cols=sql.SQL(", ").join(cols),
                vals=sql.SQL(", ").join(vals),
                conflict=conflict,
                ret=self.return_clause(),
            )

            row = await self.client.fetch_one(stmt, params, row_factory="dict", commit=False)

            if row is not None:
                res = self._decode_row(row)
                await self._write_history(res)

                return res

            current = await self._fetch_domain_by_pk(model.id, for_update=True)
            res, _ = await self.update(model.id, update, rev=current.rev)

            return res

    # ....................... #

    @postgres_occ_retry
    async def upsert_many(
        self,
        ids: Sequence[UUID],
        creates: Sequence[C],
        updates: Sequence[U],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Bulk :meth:`upsert` using batched insert-then-:meth:`update_many` for conflicts."""

        self._require_update_cmd()

        if not creates:
            return []

        async with self._write_tx():
            keys: list[str] | None = None
            col_idents: list[sql.Composable] | None = None
            row_template: sql.Composable | None = None

            def _pk_from_row(r: JsonDict) -> UUID:
                v = r[ID_FIELD]
                if isinstance(v, UUID):
                    return v
                return UUID(str(v))

            if self.serialized_by:
                creating = domains_from_create_payloads(self.create_codec, creates, ids)

                async def touched() -> Sequence[Any]:
                    stored = await self._fetch_domains_by_pks(list(ids), missing_ok=True)
                    return [*creating, *stored]

                await self._serialize(touched)

            async def _upsert_batch(
                batch: Sequence[JsonDict],
                model_batch: Sequence[D],
                u_for_batch: Sequence[U],
            ) -> list[D]:
                nonlocal keys, col_idents, row_template

                if keys is None or col_idents is None or row_template is None:
                    raise exc.internal("upsert_batch: missing required state")

                value_parts = [row_template] * len(batch)
                params_in = [b[k] for b in batch for k in keys]

                conflict = await self._ident_conflict_target()
                stmt = sql.SQL(
                    "INSERT INTO {table} ({cols}) VALUES {vals} "
                    "ON CONFLICT ({conflict}) DO NOTHING "
                    "RETURNING {ret}"
                ).format(
                    table=(await self._qname()).ident(),
                    cols=sql.SQL(", ").join(col_idents),
                    vals=sql.SQL(", ").join(value_parts),
                    conflict=conflict,
                    ret=self.return_clause(),
                )

                rows = await self.client.fetch_all(
                    stmt,
                    params_in,
                    row_factory="dict",
                    commit=False,
                )

                by_returned: dict[UUID, JsonDict] = {_pk_from_row(r): r for r in rows}

                inserted: list[D] = []

                for m in model_batch:
                    rj = by_returned.get(m.id)
                    if rj is not None:
                        inserted.append(self._decode_row(rj))

                if inserted:
                    await self._write_history(*inserted)

                need_u: list[tuple[UUID, U]] = []
                u_list = list(u_for_batch)

                for i, m in enumerate(model_batch):
                    if m.id not in by_returned:
                        need_u.append((m.id, u_list[i]))

                by_updated: dict[UUID, D] = {}

                if need_u:
                    pks_u = [a[0] for a in need_u]
                    u_dtos = [a[1] for a in need_u]
                    currents = await self._fetch_domains_by_pks(pks_u, for_update=True)
                    by_cur = {c.id: c for c in currents}
                    revs = [by_cur[pk].rev for pk in pks_u]
                    updated, _ = await self.update_many(
                        pks_u,
                        u_dtos,
                        revs=revs,
                        batch_size=batch_size,
                    )
                    by_updated = {d.id: d for d in updated}

                ordered: list[D] = []

                for m in model_batch:
                    rj = by_returned.get(m.id)

                    if rj is not None:
                        ordered.append(self._decode_row(rj))

                    else:
                        u_one = by_updated.get(m.id)

                        if u_one is None:
                            raise exc.not_found(
                                f"Record not found after upsert_many conflict: {m.id!s}",
                            )

                        ordered.append(u_one)

                return ordered

            out: list[D] = []

            for offset in range(0, len(creates), batch_size):
                id_batch = ids[offset : offset + batch_size]
                create_batch = creates[offset : offset + batch_size]
                update_batch = updates[offset : offset + batch_size]
                models = domains_from_create_payloads(self.create_codec, create_batch, id_batch)
                insert_data_raw = await self._encode_domain_many(models)
                insert_data = await self.adapt_many_payload_for_write(
                    insert_data_raw,
                    create=True,
                )

                if keys is None:
                    keys = list(insert_data[0].keys())
                    col_idents = [sql.Identifier(k) for k in keys]
                    row_template = (
                        sql.SQL("(")
                        + sql.SQL(", ").join(sql.Placeholder() for _ in keys)
                        + sql.SQL(")")
                    )

                elif list(insert_data[0].keys()) != keys:
                    raise exc.internal(
                        "upsert_many: adapted payload keys differ between batches",
                    )

                u_seq = list(update_batch)
                out.extend(await _upsert_batch(insert_data, models, u_seq))

            if len(out) != len(creates):
                raise exc.internal("upsert_many result length does not match input")

            return out

    # ....................... #

    @postgres_occ_retry
    async def upsert_many_set_based(
        self,
        ids: Sequence[UUID],
        creates: Sequence[C],
        updates: Sequence[U],
        *,
        batch_size: int = 200,
    ) -> None:
        """Insert the missing rows and patch the stored ones, a statement each per chunk.

        Nothing is read back or decoded. A missing row is the create payload's domain, as
        :meth:`upsert_many` inserts it; a stored row takes the update as its DTO encodes it,
        as :meth:`update_matching` writes one, and only where that changes it, so ``rev`` and
        ``last_update_at`` move only then. Stored rows are locked in id order before they are
        patched, and one this tenant cannot see is ``not_found``, as on the domain path. The
        caller has refused a spec whose updates need the stored row or the domain model
        (:meth:`~forze.application.contracts.document.DocumentSpec.require_set_based_upsert`);
        a relation keyed by more than the id (and tenant), or a patched column SQL cannot
        compare, takes :meth:`upsert_many` instead.
        """

        self._require_update_cmd()
        column_types = await self.column_types()

        if not await self.__fits_set_based(column_types):
            await self.upsert_many(ids, creates, updates, batch_size=batch_size)
            return

        # One order for every chunk: two calls over overlapping ids lock them in the same
        # order across chunks, so neither waits on a row the other has while holding one it
        # wants.
        items = sorted(zip(ids, creates, updates, strict=True), key=lambda item: item[0])

        async with self._write_tx():
            now = utcnow()

            for offset in range(0, len(items), batch_size):
                chunk = items[offset : offset + batch_size]
                id_batch = [pk for pk, _, _ in chunk]
                creates_by_id = {pk: create for pk, create, _ in chunk}
                updates_by_id = {pk: update for pk, _, update in chunk}

                # A create payload becomes a domain only for an id not stored yet: a reload
                # builds none.
                present = await self.__visible_ids(id_batch, lock=False)
                inserted = await self.__insert_set_based(
                    [pk for pk in id_batch if pk not in present], creates_by_id, column_types
                )
                # Everything not inserted is patched, a row a concurrent writer stored between
                # the look-up and the insert included. Locked in id order first, as the domain
                # path locks what it patches; a row gone by then (deleted, or another
                # tenant's) is not this call's to write.
                stored = [pk for pk in id_batch if pk not in inserted]

                if not stored:
                    continue

                visible = await self.__visible_ids(stored, lock=True)

                if len(visible) != len(stored):
                    raise exc.not_found("Record not found after upsert_many conflict")

                await self.__patch_stored_set_based(
                    [(pk, updates_by_id[pk], creates_by_id[pk]) for pk in stored],
                    now=now,
                    column_types=column_types,
                )

    # ....................... #

    async def __fits_set_based(self, column_types: PostgresColumnTypes) -> bool:
        """Whether a set-based upsert can key and compare every row as the domain path does."""

        keys = set(await self._resolved_conflict_target())
        keyed = keys == {ID_FIELD} or (self.tenant_aware and keys == {ID_FIELD, TENANT_ID_FIELD})
        update_cmd = cast(type[BaseDTO], self.update_cmd_type)

        return keyed and all(
            _comparable(column_types.get(name)) for name in update_cmd.model_fields
        )

    # ....................... #

    async def __visible_ids(self, pks: Sequence[UUID], *, lock: bool) -> set[UUID]:
        """The ids among *pks* stored for this tenant; locked in id order when *lock*."""

        where, params = self._add_tenant_where(
            sql.SQL("{pk} = ANY({arr})").format(pk=self.ident_pk(), arr=sql.Placeholder()),
            [list(pks)],
        )
        stmt = sql.SQL("SELECT {pk} FROM {table} WHERE {where}").format(
            pk=self.ident_pk(), table=(await self._qname()).ident(), where=where
        )

        if lock:
            stmt += sql.SQL(" ORDER BY {pk} FOR NO KEY UPDATE").format(pk=self.ident_pk())

        rows = await self.client.fetch_all(stmt, params, row_factory="dict", commit=False)

        return {UUID(str(row[ID_FIELD])) for row in rows}

    # ....................... #

    async def __insert_set_based(
        self,
        pks: Sequence[UUID],
        creates_by_id: Mapping[UUID, C],
        column_types: PostgresColumnTypes,
    ) -> set[UUID]:
        """Insert *pks* from their create payloads, skipping a conflict; the ids inserted."""

        if not pks:
            return set()

        models = domains_from_create_payloads(
            self.create_codec, [creates_by_id[pk] for pk in pks], list(pks)
        )
        rows = await self.adapt_many_payload_for_write(
            await self._encode_domain_many(models),
            create=True,
        )
        keys = list(rows[0].keys())
        source, params = _row_source(keys, rows, column_types)
        inserted = await self.client.fetch_all(
            sql.SQL(
                "INSERT INTO {table} ({cols}) {source} "
                "ON CONFLICT ({conflict}) DO NOTHING RETURNING {pk}"
            ).format(
                table=(await self._qname()).ident(),
                cols=sql.SQL(", ").join(sql.Identifier(k) for k in keys),
                source=source,
                conflict=await self._ident_conflict_target(),
                pk=self.ident_pk(),
            ),
            params,
            row_factory="dict",
            commit=False,
        )

        return {UUID(str(row[ID_FIELD])) for row in inserted}

    # ....................... #

    async def __patch_stored_set_based(
        self,
        stored: Sequence[tuple[UUID, U, C]],
        *,
        now: Any,
        column_types: PostgresColumnTypes,
    ) -> None:
        """Patch the stored rows of a set-based upsert where their update changes them."""

        pks = [pk for pk, _, _ in stored]
        patches = await self._encode_patch_many([u for _, u, _ in stored], record_ids=pks)
        fields = self.model_type.model_fields
        groups: dict[tuple[str, ...], list[JsonDict]] = defaultdict(list)

        for (pk, _, create), patch in zip(stored, patches, strict=True):
            # An explicit null on a field the domain does not let be null means what the
            # domain's update makes of it (its default, or a refusal); applied to the create
            # payload's domain, since a flat field's new value does not depend on the old.
            nulled = [
                k
                for k, v in patch.items()
                if v is None and k in fields and not _null_is_null(fields[k])
            ]

            if nulled:
                (base,) = domains_from_create_payloads(self.create_codec, [create], [pk])
                after, _ = base.update(patch, materialized=self.read_codec.materialized)
                (encoded,) = await self._encode_domain_many([after])
                patch = {**patch, **{k: encoded[k] for k in nulled}}

            adapted = dict(await self.adapt_payload_for_write(patch, create=False))

            for field in (ID_FIELD, REV_FIELD, LAST_UPDATE_AT_FIELD):
                adapted.pop(field, None)

            if adapted:
                groups[tuple(sorted(adapted))].append({ID_FIELD: pk, **adapted})

        table = (await self._qname()).ident()

        for key, batch in groups.items():
            columns = (ID_FIELD, *key)
            source, params = _row_source(columns, batch, column_types, typed_values=True)
            sets = [sql.SQL("{c} = v.{c}").format(c=sql.Identifier(k)) for k in key]
            stamp: list[Any] = []

            # Under ``"database"`` bookkeeping a trigger moves both.
            if self.strategy == "application":
                sets.append(
                    sql.SQL("{c} = {v}").format(
                        c=sql.Identifier(LAST_UPDATE_AT_FIELD), v=sql.Placeholder()
                    )
                )
                sets.append(sql.SQL("{r} = t.{r} + 1").format(r=self._ident_rev()))
                stamp.append(now)

            # Only a row the update changes is written, as the domain path skips an empty diff.
            where, where_params = self._add_tenant_where(
                sql.SQL("t.{pk} = v.{pk} AND ({tcols}) IS DISTINCT FROM ({vcols})").format(
                    pk=self.ident_pk(),
                    tcols=sql.SQL(", ").join(_compared("t", k, column_types.get(k)) for k in key),
                    vcols=sql.SQL(", ").join(_compared("v", k, column_types.get(k)) for k in key),
                ),
                [],
                table_alias="t",
            )
            await self.client.execute(
                sql.SQL(
                    "UPDATE {table} AS t SET {sets} FROM ({source}) AS v({cols}) WHERE {where}"
                ).format(
                    table=table,
                    sets=sql.SQL(", ").join(sets),
                    source=source,
                    cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                    where=where,
                ),
                [*stamp, *params, *where_params],
            )

    # ....................... #

    def __bump_rev(self, current: D, diff: JsonDict) -> JsonDict:
        if self.strategy == "application":
            diff[REV_FIELD] = current.rev + 1

        return diff

    # ....................... #

    @postgres_occ_retry
    async def __patch(
        self,
        pk: UUID,
        update: JsonDict | None = None,
        *,
        rev: int | None = None,
    ) -> tuple[D, JsonDict]:
        async with self._write_tx():

            async def touched() -> Sequence[Any]:
                stored = await self.read_gw.get(pk)
                return [stored, self._moved(stored, update)]

            await self._serialize(touched)
            current = await self.read_gw.get(pk)

            if update is not None:
                if rev is not None:
                    await self._validate_history((current, rev, update))

                _, diff = current.update(update, materialized=self.read_codec.materialized)

            else:
                # Always historically consistent because we update only the revision and update timestamp
                _, diff = current.touch()

            if not diff:
                return current, diff

            diff = self.__bump_rev(current, diff)

            diff = await self.adapt_payload_for_write(diff, create=False)
            set_parts: list[sql.Composable] = []
            params: list[Any] = []

            for k, v in diff.items():
                set_parts.append(sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder()))
                params.append(v)

            where_sql = self._where_pk_rev()
            where_params: list[Any] = [current.id, current.rev]
            where_sql, where_params = self._add_tenant_where(where_sql, where_params)  # type: ignore[assignment]
            params.extend(where_params)

            stmt = sql.SQL("UPDATE {table} SET {sets} WHERE {where} RETURNING {ret}").format(
                table=(await self._qname()).ident(),
                sets=sql.SQL(", ").join(set_parts),
                where=where_sql,
                ret=self.return_clause(),
            )

            row = await self.client.fetch_one(stmt, params, row_factory="dict", commit=False)

            if row is None:
                raise exc.concurrency("Failed to update record")

            res = self._decode_row(row)
            await self._write_history(res)

            return res, diff

    # ....................... #

    async def update(
        self,
        pk: UUID,
        dto: U,
        *,
        rev: int | None = None,
    ) -> tuple[D, JsonDict]:
        self._require_update_cmd()

        update_data = await self._encode_patch_one(dto, record_id=pk)

        return await self.__patch(pk, update_data, rev=rev)

    # ....................... #

    async def touch(self, pk: UUID) -> D:
        res, _ = await self.__patch(pk)

        return res

    # ....................... #

    @postgres_occ_retry
    async def __patch_group(
        self,
        key: tuple[str, ...],
        batch: list[tuple[UUID, int, JsonDict]],
    ) -> list[D]:
        # First two VALUES columns are the PK and the *expected* revision for the WHERE
        # clause. When the patch bumps ``rev``, the diff also contains a new ``rev`` value
        # for SET; naming the match column ``expected_rev`` avoids duplicate ``rev`` in
        # ``AS v(...)``, which PostgreSQL rejects as ambiguous.
        expected_rev_alias = "expected_rev"
        value_cols = [ID_FIELD, expected_rev_alias, *list(key)]
        v_col_idents: list[sql.Composable] = [
            self.ident_pk(),
            sql.Identifier(expected_rev_alias),
            *(sql.Identifier(k) for k in key),
        ]
        values_rows: list[sql.Composable] = []
        params: list[Any] = []

        column_types = await self.column_types()

        # ⚡ Bolt: Precompute the row template to avoid repeatedly instantiating
        # sql.SQL and parsing it for every record in the batch, improving CPU bound performance
        row_template = (
            sql.SQL("(")
            + sql.SQL(", ").join(
                _values_placeholder_for_patch_group(
                    column=c,
                    expected_rev_alias=expected_rev_alias,
                    column_types=column_types,
                )
                for c in value_cols
            )
            + sql.SQL(")")
        )

        for _id, _rev, d in batch:
            row_params = [_id, _rev] + [d[k] for k in key]
            params.extend(row_params)
            values_rows.append(row_template)

        where_sql = sql.SQL("t.{tpk} = v.{vpk} AND t.{trev} = v.{vexp}").format(
            tpk=self.ident_pk(),
            vpk=self.ident_pk(),
            trev=self._ident_rev(),
            vexp=sql.Identifier(expected_rev_alias),
        )
        where_params: list[Any] = []
        where_sql, where_params = self._add_tenant_where(  # type: ignore[assignment]
            where_sql,
            where_params,
            table_alias="t",
        )
        params.extend(where_params)

        set_parts = [sql.SQL("{c} = v.{c}").format(c=sql.Identifier(k)) for k in key]

        stmt = sql.SQL(
            """
            UPDATE {table} AS t
            SET {sets}
            FROM (VALUES {vals}) AS v({cols})
            WHERE {where}
            RETURNING {ret}
            """
        ).format(
            table=(await self._qname()).ident(),
            sets=sql.SQL(", ").join(set_parts),
            vals=sql.SQL(", ").join(values_rows),
            cols=sql.SQL(", ").join(v_col_idents),
            where=where_sql,
            ret=self.return_clause(table_alias="t"),
        )

        rows = await self.client.fetch_all(
            stmt,
            params,
            row_factory="dict",
            commit=False,
        )
        updated_ids = {row[ID_FIELD] for row in rows}
        expected_ids = {_id for _id, _, _ in batch}

        missing = expected_ids - updated_ids

        if missing:
            raise exc.concurrency("Failed to update records")

        return self._decode_rows(rows)

    # ....................... #

    async def __patch_many(
        self,
        pks: Sequence[UUID],
        updates: Sequence[JsonDict] | None = None,
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 200,
    ) -> tuple[Sequence[D], Sequence[JsonDict]]:
        if not pks or (not updates and updates is not None):
            return [], []

        if updates is not None and len(pks) != len(updates):
            raise exc.internal("Length mismatch between primary keys and updates")

        if len(pks) != len(set(pks)):
            raise exc.precondition("Primary keys must be unique")

        async with self._write_tx():

            async def touched() -> Sequence[Any]:
                stored = await self.read_gw.get_many(pks)

                if updates is None:
                    return stored

                moved = [self._moved(c, u) for c, u in zip(stored, updates, strict=True)]

                return [*stored, *moved]

            await self._serialize(touched)
            currents = await self.read_gw.get_many(pks)

            groups: dict[tuple[str, ...], list[tuple[UUID, int, JsonDict]]] = defaultdict(list)

            if updates is None:

                async def _prepare_touch(c: D) -> tuple[UUID, int, JsonDict]:
                    _, diff = c.touch()
                    diff = self.__bump_rev(c, diff)
                    adapted_diff = await self.adapt_payload_for_write(diff, create=False)

                    return c.id, c.rev, adapted_diff

                results = await gather_db_work(
                    self.client,
                    [partial(_prepare_touch, c) for c in currents],
                )
                for cid, crev, diff in results:
                    # always the same key so we can handle only one group
                    key = tuple(sorted(diff.keys()))
                    groups[key].append((cid, crev, diff))

            else:
                # if revisions are provided, validate historical consistency
                if revs is not None:
                    data = [
                        (c, r, u)
                        for c, r, u in zip(
                            currents,
                            revs,
                            updates,
                            strict=True,
                        )
                    ]
                    await self._validate_history(*data)

                async def _prepare_update(
                    c: D,
                    u: JsonDict,
                ) -> tuple[UUID, int, JsonDict] | None:
                    _, diff = c.update(u, materialized=self.read_codec.materialized)
                    if not diff:
                        return None

                    diff = self.__bump_rev(c, diff)

                    return (
                        c.id,
                        c.rev,
                        await self.adapt_payload_for_write(diff, create=False),
                    )

                results = await gather_db_work(
                    self.client,
                    [
                        partial(_prepare_update, c, u)  # type: ignore[misc]
                        for c, u in zip(currents, updates, strict=True)
                    ],
                )
                for r in results:
                    if r:
                        cid, crev, diff = r
                        # always the same key so we can handle only one group
                        key = tuple(sorted(diff.keys()))
                        groups[key].append((cid, crev, diff))

            if not groups:
                return currents, [{} for _ in currents]

            updated_models: dict[UUID, D] = {}
            update_diffs: dict[UUID, JsonDict] = {}
            work: list[tuple[tuple[str, ...], list[tuple[UUID, int, JsonDict]]]] = []

            for fields_key, rows in groups.items():
                for start in range(0, len(rows), batch_size):
                    work.append((fields_key, rows[start : start + batch_size]))

            batch_results = await gather_db_work(
                self.client,
                [
                    partial(
                        self.__patch_group,
                        fk,
                        bb,
                    )
                    for fk, bb in work
                ],
            )
            for (_, batch), updated in zip(work, batch_results, strict=True):
                updated_models.update({m.id: m for m in updated})

                # RETURNING rows carry no order guarantee, so key each diff by
                # its own record id instead of pairing positionally
                for cid, _, diff in batch:
                    update_diffs[cid] = diff

            res = [updated_models.get(c.id, c) for c in currents]
            res_diffs = [update_diffs.get(c.id, {}) for c in res]

            await self._write_history(*res)

            return res, res_diffs

    # ....................... #

    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 200,
    ) -> tuple[Sequence[D], Sequence[JsonDict]]:

        self._require_update_cmd()

        updates: list[JsonDict] = []
        for start in range(0, len(dtos), batch_size):
            stop = start + batch_size
            updates.extend(
                await self._encode_patch_many(dtos[start:stop], record_ids=pks[start:stop]),
            )

        res, res_diffs = await self.__patch_many(
            pks,
            updates,
            revs=revs,
            batch_size=batch_size,
        )

        return res, res_diffs

    # ....................... #

    @postgres_occ_retry
    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        batch_size: int = 200,
    ) -> tuple[int, Sequence[D]]:
        """Bulk-update rows matching *filters*, keyset-paged in ``batch_size`` chunks.

        Each chunk updates one primary-key page (``id`` ascending) with its own
        ``UPDATE … RETURNING`` and history write, so a broad filter never runs a
        single unbounded statement; the whole loop is one transaction. Revision is
        bumped with ``rev = rev + 1`` when :attr:`strategy` is ``"application"``; for
        ``"database"`` the revision is left to triggers.

        The matched primary keys are snapshotted into memory (to keep the set stable
        across chunks); a filter matching more than :attr:`update_matching_max_rows`
        fails with ``precondition`` rather than pulling an unbounded key set in.
        """

        self._require_update_cmd()
        self._reject_matching_update_with_materialized()

        if batch_size < 1:
            raise exc.internal("batch_size must be >= 1")

        update_data = await self._encode_patch_one(dto)

        if not update_data:
            return 0, []

        adapted = dict(await self.adapt_payload_for_write(update_data, create=False))
        adapted.pop(REV_FIELD, None)

        if not adapted:
            return 0, []

        async with self._write_tx():
            set_parts: list[sql.Composable] = []
            set_params: list[Any] = []

            for k, v in adapted.items():
                set_parts.append(sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder()))
                set_params.append(v)

            if self.strategy == "application":
                set_parts.append(
                    sql.SQL("{} = {} + 1").format(
                        self._ident_rev(),
                        self._ident_rev(),
                    )
                )

            where_sql, where_params = await self.where_clause(filters)
            table = (await self._qname()).ident()
            pk = self.ident_pk()
            sets = sql.SQL(", ").join(set_parts)
            ret = self.return_clause()

            def _pk_from_row(r: JsonDict) -> UUID:
                v = r[ID_FIELD]

                return v if isinstance(v, UUID) else UUID(str(v))

            # Snapshot the matching primary keys once, then update them in
            # ``batch_size`` chunks (history written per chunk) so a broad filter
            # never drives a single unbounded ``UPDATE … RETURNING``. Freezing the id
            # set up front keeps the matched set stable across chunks: re-evaluating
            # ``WHERE filters`` per chunk under READ COMMITTED would drift as rows are
            # concurrently inserted/updated, unlike the prior single statement. The
            # whole thing runs in one transaction (``_write_tx``), so it stays atomic.
            id_stmt = sql.SQL("SELECT {pk} FROM {table} WHERE {where} ORDER BY {pk}").format(
                pk=pk, table=table, where=where_sql
            )
            id_params = list(where_params)

            # Bound the id snapshot: fetch at most ``max_rows + 1`` (a probe) so an
            # over-broad filter fails closed instead of pulling an unbounded key set into
            # memory. ``None`` opts into the unbounded snapshot.
            cap = self.update_matching_max_rows

            if cap is not None:
                id_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
                id_params.append(cap + 1)

            async def matching_ids() -> list[UUID]:
                id_rows = await self.client.fetch_all(
                    id_stmt,
                    id_params,
                    row_factory="dict",
                    commit=False,
                )

                if cap is not None and len(id_rows) > cap:
                    raise exc.precondition(
                        f"update_matching would touch more than {cap} rows; narrow the "
                        "filter or paginate (or raise/disable update_matching_max_rows).",
                        code="core.document.update_matching_too_broad",
                    )

                return [_pk_from_row(r) for r in id_rows]

            ids: list[UUID] = []

            # Every owner the filter selects and every owner the patch would move a row to.
            # The set updated is the one the last pass read, whose owners are all held.
            async def touched() -> Sequence[Any]:
                nonlocal ids
                ids = await matching_ids()
                stored = await self._fetch_domains_by_pks(ids, missing_ok=True)

                return [*stored, *(self._moved(d, update_data) for d in stored)]

            if self.serialized_by:
                await self._serialize(touched)

            else:
                ids = await matching_ids()

            total = 0
            out_domains: list[D] = []

            for start in range(0, len(ids), batch_size):
                chunk = ids[start : start + batch_size]
                stmt = sql.SQL(
                    "UPDATE {table} SET {sets} WHERE {pk} = ANY({ids}) RETURNING {ret}"
                ).format(table=table, sets=sets, pk=pk, ids=sql.Placeholder(), ret=ret)

                rows = await self.client.fetch_all(
                    stmt,
                    [*set_params, chunk],
                    row_factory="dict",
                    commit=False,
                )

                if not rows:
                    continue

                doms = self._decode_rows(rows)
                await self._write_history(*doms)
                out_domains.extend(doms)
                total += len(doms)

            return total, out_domains

    # ....................... #

    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        res, _ = await self.__patch_many(pks, None, batch_size=batch_size)

        return res

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        where_sql = sql.SQL("{pk} = {value}").format(
            pk=self.ident_pk(),
            value=sql.Placeholder(),
        )
        params: list[Any] = [pk]
        where_sql, params = self._add_tenant_where(where_sql, params)  # type: ignore[assignment]

        stmt = sql.SQL("DELETE FROM {table} WHERE {where}").format(
            table=(await self._qname()).ident(),
            where=where_sql,
        )

        # A plain delete is one statement; a serialized one has to hold its owner across the
        # read that names it, which takes a transaction.
        async with self._write_tx() if self.serialized_by else nullcontext():

            async def touched() -> Sequence[Any]:
                return await self._fetch_domains_by_pks([pk], missing_ok=True)

            await self._serialize(touched)
            n = await self.client.execute(stmt, params, return_rowcount=True)

        if n == 0:
            raise exc.not_found(f"Record not found: {pk}")

    # ....................... #

    async def kill_many(
        self,
        pks: Sequence[UUID],
        *,
        batch_size: int = 200,
    ) -> None:
        if not pks:
            return

        if len(pks) != len(set(pks)):
            raise exc.precondition("Primary keys must be unique")

        async with self._write_tx():

            async def touched() -> Sequence[Any]:
                return await self._fetch_domains_by_pks(list(pks), missing_ok=True)

            await self._serialize(touched)
            where_sql = sql.SQL("{pk} = ANY({ids})").format(
                pk=self.ident_pk(),
                ids=sql.Placeholder(),
            )
            trailing_params: list[Any] = []
            where_sql, trailing_params = self._add_tenant_where(  # type: ignore[assignment]
                where_sql,
                trailing_params,
            )

            stmt = sql.SQL("DELETE FROM {table} WHERE {where}").format(
                table=(await self._qname()).ident(),
                where=where_sql,
            )

            async def _delete_batch(batch: list[UUID]) -> None:
                params: list[Any] = [list(batch), *trailing_params]

                n = await self.client.execute(stmt, params, return_rowcount=True)

                if n != len(batch):
                    if self.tenant_aware:
                        raise exc.not_found(
                            "Some records not found or not accessible in this tenant scope"
                        )

                    raise exc.not_found("Some records not found")

            batches = [
                list(pks[start : start + batch_size]) for start in range(0, len(pks), batch_size)
            ]

            await gather_db_work(
                self.client,
                [partial(_delete_batch, b) for b in batches],
            )
