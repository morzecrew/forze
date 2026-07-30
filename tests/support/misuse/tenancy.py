"""N family — data & multitenancy misuse: the dropped tenant predicate and the stale cache.

N1: tenant 0 writes rows, tenant 1 browses. The correct browse filters by the viewer's tenant
and always sees zero; the mutant drops the predicate and sees the other tenant's rows — the
cross-tenant leak, observable as a browse-log row with a non-zero count. N2: a writer bumps the
source version; the correct twin updates the cache in the same transaction; the mutant drops the
write-path invalidation, so even the writer's own read-through sees the stale version
(read-your-writes broken — the final-state-safe staleness observable).
"""

from __future__ import annotations

from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation
from forze_dst.invariants import expect
from forze_dst.markers import record_event
from forze_dst.misuse import MisuseCase
from forze_mock import MockDepsModule

# ----------------------- #
# N1 — the dropped tenant predicate.


class TenantRow(Document):
    tenant: int


class TenantRowCreate(CreateDocumentCmd):
    tenant: int


class TenantRowRead(ReadDocument):
    tenant: int


class BrowseLog(Document):
    viewer: int
    seen: int


class BrowseLogCreate(CreateDocumentCmd):
    viewer: int
    seen: int


class BrowseLogRead(ReadDocument):
    viewer: int
    seen: int


TENANT_ROW_SPEC = DocumentSpec(
    name="tenant_rows",
    read=TenantRowRead,
    write=DocumentWriteTypes(domain=TenantRow, create_cmd=TenantRowCreate),
)
BROWSE_SPEC = DocumentSpec(
    name="browse_log",
    read=BrowseLogRead,
    write=DocumentWriteTypes(domain=BrowseLog, create_cmd=BrowseLogCreate),
)

OWNER, VIEWER = 0, 1


class PutCmd(BaseModel):
    tenant: int


class BrowseCmd(BaseModel):
    viewer: int


@attrs.define(slots=True, kw_only=True)
class _Put(Handler[PutCmd, None]):
    ctx: ExecutionContext

    async def __call__(self, args: PutCmd) -> None:
        async with self.ctx.tx_ctx.scope("mock"):
            await self.ctx.document.command(TENANT_ROW_SPEC).create(
                TenantRowCreate(tenant=args.tenant)
            )


@attrs.define(slots=True, kw_only=True)
class _Browse(Handler[BrowseCmd, None]):
    """``filtered=False`` is the MUTANT (N1 drop_tenant_predicate)."""

    ctx: ExecutionContext
    filtered: bool

    async def __call__(self, args: BrowseCmd) -> None:
        async with self.ctx.tx_ctx.scope("mock"):
            if self.filtered:
                seen = await self.ctx.document.query(TENANT_ROW_SPEC).count(
                    {"$values": {"tenant": args.viewer}}
                )
            else:
                # MUTANT (N1 drop_tenant_predicate): the tenant filter is gone — the browse
                # counts every tenant's rows.
                seen = await self.ctx.document.query(TENANT_ROW_SPEC).count()
            await self.ctx.document.command(BROWSE_SPEC).create(
                BrowseLogCreate(viewer=args.viewer, seen=seen)
            )


def _browse_case(*, filtered: bool) -> MisuseCase:
    registry = OperationRegistry(
        handlers={
            "put": lambda ctx: _Put(ctx=ctx),
            "browse": lambda ctx: _Browse(ctx=ctx, filtered=filtered),
        },
        descriptors={
            "put": OperationDescriptor(input_type=PutCmd, output_type=None, description="Put."),
            "browse": OperationDescriptor(
                input_type=BrowseCmd, output_type=None, description="Browse own rows."
            ),
        },
    ).freeze()

    async def observe(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            leaks = await ctx.document.query(BROWSE_SPEC).count(
                {"$values": {"viewer": VIEWER, "seen": {"$gt": 0}}}
            )
        record_event("tenant_leaks", total=leaks)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            observe=observe,
            invariants=[
                expect(
                    "tenant_leaks",
                    lambda e: e.fields["total"] == 0,
                    message="a browse saw another tenant's rows (cross-tenant leak)",
                )
            ],
        ),
        scenario=Scenario(
            state=ModelState,
            act=(
                Rule(op="put", arg=lambda _state, _rng: PutCmd(tenant=OWNER)),
                Rule(op="browse", arg=lambda _state, _rng: BrowseCmd(viewer=VIEWER)),
            ),
        ),
    )


def n1_drop_tenant_predicate() -> MisuseCase:
    return _browse_case(filtered=False)


def ctrl_tenant_filtered_browse() -> MisuseCase:
    return _browse_case(filtered=True)


# ....................... #
# N2 — the stale read-through cache (write-path invalidation dropped).


class Source(Document):
    version: int


class SourceCreate(CreateDocumentCmd):
    version: int


class SourceUpdate(BaseDTO):
    version: int | None = None


class SourceRead(ReadDocument):
    version: int


class CacheRow(Document):
    version: int


class CacheRowCreate(CreateDocumentCmd):
    version: int


class CacheRowUpdate(BaseDTO):
    version: int | None = None


class CacheRowRead(ReadDocument):
    version: int


class RwLog(Document):
    written: int
    seen: int


class RwLogCreate(CreateDocumentCmd):
    written: int
    seen: int


class RwLogRead(ReadDocument):
    written: int
    seen: int


SOURCE_SPEC = DocumentSpec(
    name="cache_sources",
    read=SourceRead,
    write=DocumentWriteTypes(domain=Source, create_cmd=SourceCreate, update_cmd=SourceUpdate),
)
CACHE_SPEC = DocumentSpec(
    name="cache_rows",
    read=CacheRowRead,
    write=DocumentWriteTypes(domain=CacheRow, create_cmd=CacheRowCreate, update_cmd=CacheRowUpdate),
)
RW_LOG_SPEC = DocumentSpec(
    name="rw_log",
    read=RwLogRead,
    write=DocumentWriteTypes(domain=RwLog, create_cmd=RwLogCreate),
)

SOURCE_ID = UUID(int=70001)
CACHE_ID = UUID(int=70002)


class BumpCmd(BaseModel):
    pass


@attrs.define(slots=True, kw_only=True)
class _BumpThenRead(Handler[BumpCmd, None]):
    """``invalidate=False`` is the MUTANT (N2 stale_cache)."""

    ctx: ExecutionContext
    invalidate: bool

    async def __call__(self, _args: BumpCmd) -> None:
        async with self.ctx.tx_ctx.scope("mock"):
            source = await self.ctx.document.query(SOURCE_SPEC).get(SOURCE_ID)
            written = source.version + 1
            await self.ctx.document.command(SOURCE_SPEC).update(
                SOURCE_ID, source.rev, SourceUpdate(version=written)
            )
            if self.invalidate:
                cache = await self.ctx.document.query(CACHE_SPEC).get(CACHE_ID)
                await self.ctx.document.command(CACHE_SPEC).update(
                    CACHE_ID, cache.rev, CacheRowUpdate(version=written)
                )
            # MUTANT (N2 stale_cache): the write path never touches the cache.

        async with self.ctx.tx_ctx.scope("mock"):
            cached = await self.ctx.document.query(CACHE_SPEC).get(CACHE_ID)
            await self.ctx.document.command(RW_LOG_SPEC).create(
                RwLogCreate(written=written, seen=cached.version)
            )


def _cache_case(*, invalidate: bool) -> MisuseCase:
    registry = OperationRegistry(
        handlers={"bump": lambda ctx: _BumpThenRead(ctx=ctx, invalidate=invalidate)},
        descriptors={
            "bump": OperationDescriptor(
                input_type=BumpCmd, output_type=None, description="Bump then read through."
            )
        },
    ).freeze()

    async def setup(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            await ctx.document.command(SOURCE_SPEC).create(SourceCreate(version=0), id=SOURCE_ID)
            await ctx.document.command(CACHE_SPEC).create(CacheRowCreate(version=0), id=CACHE_ID)

    async def observe(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            stale = await ctx.document.query(RW_LOG_SPEC).count(
                {"$values": {"seen": {"$lt": 1}, "written": {"$gt": 0}}}
            )
        record_event("stale_reads", total=stale)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            setup=setup,
            observe=observe,
            invariants=[
                expect(
                    "stale_reads",
                    lambda e: e.fields["total"] == 0,
                    message="a writer's own read-through saw the stale cached version",
                )
            ],
        ),
        scenario=Scenario(
            state=ModelState, act=(Rule(op="bump", arg=lambda _state, _rng: BumpCmd()),)
        ),
    )


def n2_stale_cache() -> MisuseCase:
    return _cache_case(invalidate=False)


def ctrl_cache_invalidate_in_tx() -> MisuseCase:
    return _cache_case(invalidate=True)


# ....................... #
# N3 — cursor_unbound_tenant: the paged walk. Page 1 filters by the viewer's tenant and takes a
# cursor; the continuation trusts the cursor as a self-contained query handle and drops the
# tenant predicate — so the keyset resume walks straight into the other tenant's rows. The
# correct twin re-applies the predicate with the same cursor. Items interleave across tenants in
# the sort order, so an unbound resume leaks deterministically.


class CatalogRow(Document):
    tenant: int
    item: int


class CatalogRowCreate(CreateDocumentCmd):
    tenant: int
    item: int


class CatalogRowRead(ReadDocument):
    tenant: int
    item: int


CATALOG_SPEC = DocumentSpec(
    name="catalog_rows",
    read=CatalogRowRead,
    write=DocumentWriteTypes(domain=CatalogRow, create_cmd=CatalogRowCreate),
)


class WalkCmd(BaseModel):
    viewer: int


@attrs.define(slots=True, kw_only=True)
class _WalkPages(Handler[WalkCmd, None]):
    """``bound=False`` is the MUTANT (N3 cursor_unbound_tenant): the continuation query keeps
    the cursor but drops the tenant predicate."""

    ctx: ExecutionContext
    bound: bool

    async def __call__(self, args: WalkCmd) -> None:
        async with self.ctx.tx_ctx.scope("mock"):
            query = self.ctx.document.query(CATALOG_SPEC)
            first = await query.find_cursor(
                {"$values": {"tenant": args.viewer}},
                cursor={"limit": 2},
                sorts={"item": "asc"},
            )
            if first.next_cursor is None:
                return

            if self.bound:
                resumed = await query.find_cursor(
                    {"$values": {"tenant": args.viewer}},
                    cursor={"limit": 4, "after": first.next_cursor},
                    sorts={"item": "asc"},
                )
            else:
                # MUTANT (N3 cursor_unbound_tenant): the cursor is treated as a
                # self-contained handle — the resume walks every tenant's rows.
                resumed = await query.find_cursor(
                    cursor={"limit": 4, "after": first.next_cursor},
                    sorts={"item": "asc"},
                )

            foreign = sum(1 for row in resumed.hits if row.tenant != args.viewer)
            await self.ctx.document.command(BROWSE_SPEC).create(
                BrowseLogCreate(viewer=args.viewer, seen=foreign)
            )


_WALK_SCENARIO = Scenario(
    state=ModelState,
    act=(Rule(op="walk", arg=lambda _state, _rng: WalkCmd(viewer=OWNER)),),
)


def _walk_case(*, bound: bool) -> MisuseCase:
    registry = OperationRegistry(
        handlers={"walk": lambda ctx: _WalkPages(ctx=ctx, bound=bound)},
        descriptors={
            "walk": OperationDescriptor(
                input_type=WalkCmd, output_type=None, description="Walk own catalog pages."
            )
        },
    ).freeze()

    async def setup(ctx: ExecutionContext) -> None:
        # Interleaved sort order: the viewer's items are even, the other tenant's odd.
        async with ctx.tx_ctx.scope("mock"):
            command = ctx.document.command(CATALOG_SPEC)
            for item in range(8):
                await command.create(
                    CatalogRowCreate(tenant=item % 2, item=item), id=UUID(int=60000 + item)
                )

    async def observe(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            leaks = await ctx.document.query(BROWSE_SPEC).count(
                {"$values": {"viewer": OWNER, "seen": {"$gt": 0}}}
            )
        record_event("cursor_leaks", total=leaks)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            setup=setup,
            observe=observe,
            invariants=[
                expect(
                    "cursor_leaks",
                    lambda e: e.fields["total"] == 0,
                    message="a cursor resume walked another tenant's rows (token not bound "
                    "to the tenant predicate)",
                )
            ],
        ),
        scenario=_WALK_SCENARIO,
    )


def n3_unbound_cursor_walk() -> MisuseCase:
    return _walk_case(bound=False)


def ctrl_bound_cursor_walk() -> MisuseCase:
    return _walk_case(bound=True)
