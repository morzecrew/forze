"""The audit hooks against real Postgres: where each row is written is observable here.

The unit battery runs on the mock, whose transactions roll back in memory and whose read-only
flag is a Python bit. Here the business write and the audit row share a real transaction, a
read runs in a real ``READ ONLY`` transaction that would reject an ``INSERT``, and metadata
round-trips through ``jsonb`` — the three places a mock can agree with code a database refuses.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest
import pytest_asyncio
from pydantic import BaseModel

from forze.application.contracts.audit import AuditOutcome, AuditSpec
from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.execution import BeforeStep
from forze.application.contracts.transaction.deps import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.hooks.audit import Audited
from forze.base.exceptions import CoreException, exc
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_kits.integrations.audit import AuditDepsModule, audit_record_spec
from forze_postgres.execution.deps import ConfigurablePostgresDocument, postgres_txmanager
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.integration

# ----------------------- #

USER = uuid4()
OTHER = uuid4()

# The audit collection's DDL, as the docs page hands it to applications (not tenant-aware here).
_AUDIT_DDL = """
CREATE TABLE audit_events (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    action text NOT NULL,
    outcome text NOT NULL,
    actor_id uuid,
    subject_id uuid,
    object_type text,
    object_id text,
    metadata jsonb NOT NULL DEFAULT '{}',
    at timestamptz NOT NULL
);
"""

_THINGS_DDL = """
CREATE TABLE audit_things (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    label text NOT NULL
);
"""


class _Thing(Document):
    label: str


class _ThingCreate(BaseDTO):
    label: str


class _ThingRead(ReadDocument):
    label: str


THINGS = DocumentSpec(
    name="things",
    read=_ThingRead,
    write=DocumentWriteTypes(domain=_Thing, create_cmd=_ThingCreate),
)
TRAIL = audit_record_spec()


class _Args(BaseModel):
    label: str = "a"
    boom: bool = False
    owner: UUID | None = None


@attrs.define(slots=True, kw_only=True, frozen=True)
class _Create:
    ctx: ExecutionContext

    async def __call__(self, args: _Args) -> _ThingRead:
        thing = await self.ctx.document.command(THINGS).create(_ThingCreate(label=args.label))

        if args.boom:
            raise RuntimeError("handler failed after its write")

        return thing


@attrs.define(slots=True, kw_only=True, frozen=True)
class _Read:
    ctx: ExecutionContext

    async def __call__(self, args: _Args) -> _Args:
        return args


def _document(table: str) -> ConfigurablePostgresDocument[Any, Any, Any, Any]:
    return ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
    )


def _ctx(pg_client: PostgresClient) -> ExecutionContext:
    documents = {"things": _document("audit_things"), "audit_events": _document("audit_events")}

    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
        .merge(
            Deps.routed(
                {
                    DocumentQueryDepKey: documents,
                    DocumentCommandDepKey: documents,
                    TransactionManagerDepKey: {"main": postgres_txmanager},
                }
            )
        )
        .merge(AuditDepsModule(tx_route="main")())
    )


def _registry(audited: Audited, *, read: bool = False, deny: bool = False) -> Any:
    handler = _Read if read else _Create
    binder = OperationRegistry(handlers={"op": lambda ctx: handler(ctx=ctx)}).bind("op")

    if read:
        binder = binder.as_query()

    binder = binder.bind_tx().set_route("main").finish()

    if deny:

        def _guard(_ctx: ExecutionContext) -> Any:
            async def _before(_args: Any) -> None:
                raise exc.authorization("no")

            return _before

        binder = binder.bind_outer().before(BeforeStep(id="guard", factory=_guard)).finish()

    return audited.bind(binder).finish().freeze()


async def _run(reg: Any, ctx: ExecutionContext, args: _Args | None = None) -> Any:
    with ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=USER)):
        return await run_operation(reg, "op", args if args is not None else _Args(), ctx)


async def _rows(pg_client: PostgresClient) -> list[dict[str, Any]]:
    return list(await pg_client.fetch_all("SELECT outcome, metadata FROM audit_events"))


async def _things(pg_client: PostgresClient) -> int:
    return len(await pg_client.fetch_all("SELECT id FROM audit_things"))


@pytest_asyncio.fixture(autouse=True)
async def _tables(pg_client: PostgresClient):
    await pg_client.execute("DROP TABLE IF EXISTS audit_events; DROP TABLE IF EXISTS audit_things;")
    await pg_client.execute(_AUDIT_DDL)
    await pg_client.execute(_THINGS_DDL)
    yield


# ....................... #


SPEC = AuditSpec(action="thing.create")


async def test_an_admitted_write_and_its_row_commit_together(pg_client: PostgresClient) -> None:
    await _run(_registry(Audited(spec=SPEC)), _ctx(pg_client))

    assert [row["outcome"] for row in await _rows(pg_client)] == ["allowed"]
    assert await _things(pg_client) == 1


async def test_a_failed_audit_write_rolls_the_business_write_back(
    pg_client: PostgresClient,
) -> None:
    await pg_client.execute("DROP TABLE audit_events;")

    with pytest.raises(CoreException):
        await _run(_registry(Audited(spec=SPEC)), _ctx(pg_client))

    assert await _things(pg_client) == 0


async def test_a_failed_write_rolls_back_and_its_row_is_committed_after(
    pg_client: PostgresClient,
) -> None:
    with pytest.raises(RuntimeError):
        await _run(_registry(Audited(spec=SPEC)), _ctx(pg_client), _Args(boom=True))

    assert [row["outcome"] for row in await _rows(pg_client)] == ["failed"]
    assert await _things(pg_client) == 0


async def test_a_denial_is_recorded(pg_client: PostgresClient) -> None:
    with pytest.raises(CoreException):
        await _run(_registry(Audited(spec=SPEC), deny=True), _ctx(pg_client))

    assert [row["outcome"] for row in await _rows(pg_client)] == ["denied"]


async def test_a_read_is_recorded_outside_its_read_only_transaction(
    pg_client: PostgresClient,
) -> None:
    # Written inside the read's own transaction, the INSERT would hit READ ONLY and fail.
    audited = Audited(spec=AuditSpec(action="thing.read"), owner=lambda args, result: result.owner)

    await _run(_registry(audited, read=True), _ctx(pg_client), _Args(owner=OTHER))

    assert [row["outcome"] for row in await _rows(pg_client)] == [AuditOutcome.ALLOWED.value]


async def test_every_metadata_scalar_survives_jsonb(pg_client: PostgresClient) -> None:
    ref = uuid4()
    audited = Audited(
        spec=AuditSpec(
            action="thing.create",
            allowed_metadata=frozenset({"text", "count", "ratio", "flag", "ref", "none"}),
        ),
        metadata=lambda args, result: {
            "text": "x",
            "count": 3,
            "ratio": 0.5,
            "flag": True,
            "ref": ref,
            "none": None,
        },
    )
    ctx = _ctx(pg_client)

    await _run(_registry(audited), ctx)

    [row] = (await ctx.document.query(TRAIL).find_many()).hits
    assert row.metadata == {
        "text": "x",
        "count": 3,
        "ratio": 0.5,
        "flag": True,
        "ref": str(ref),
        "none": None,
    }
    assert type(row.metadata["flag"]) is bool and type(row.metadata["count"]) is int
