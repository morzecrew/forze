"""Real-Postgres offline mailbox: the document-backed mailbox + cursors over two
**tenant-aware** Postgres collections.

Proves the document logic the mock-backed unit tests assert (ordering, since-cursor,
tenant isolation, monotonic + min cursor, ack-trim) holds against a real adapter, with
tenancy enforced by the adapter (the injected ``tenant_id`` column) — the kit carries
no tenant code.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
import pytest_asyncio

from forze.application.contracts.crypto import (
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import CryptoDepsModule, Deps, ExecutionContext
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import (
    build_realtime_cursors,
    build_realtime_mailbox,
    realtime_mailbox_spec,
)
from forze_mock import MockKeyManagement
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps
from tests.support.realtime_retention import UNSWEPT

pytestmark = pytest.mark.integration

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")

# tenant_id is the adapter-managed scoping column (no model field); rest mirrors the models.
_MAILBOX_DDL = """
CREATE TABLE rt_mailbox (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    tenant_id uuid NOT NULL,
    principal text NOT NULL,
    event_id text NOT NULL,
    hlc bigint NOT NULL,
    event text NOT NULL,
    payload jsonb NOT NULL
);
"""

_CURSORS_DDL = """
CREATE TABLE rt_cursors (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    tenant_id uuid NOT NULL,
    principal text NOT NULL,
    client_key text NOT NULL,
    hlc bigint NOT NULL
);
"""


def _hlc(physical_ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=physical_ms, logical=0)


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": text})


def _eid(n: int) -> str:
    return str(UUID(int=n))


def _bind(ctx: ExecutionContext, tenant: UUID):  # type: ignore[no-untyped-def]
    return ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant))


def _configurable(table: str) -> ConfigurablePostgresDocument:
    return ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
            tenant_aware=True,  # the adapter injects/filters tenant_id — kit stays tenant-free
        )
    )


@pytest.fixture
def mailbox_ctx(pg_client: PostgresClient) -> ExecutionContext:
    return context_from_deps(
        Deps.merge(
            Deps.plain(
                {
                    PostgresClientDepKey: pg_client,
                    PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                }
            ),
            Deps.routed(
                {
                    DocumentQueryDepKey: {
                        "realtime-mailbox": _configurable("rt_mailbox"),
                        "realtime-cursors": _configurable("rt_cursors"),
                    },
                    DocumentCommandDepKey: {
                        "realtime-mailbox": _configurable("rt_mailbox"),
                        "realtime-cursors": _configurable("rt_cursors"),
                    },
                }
            ),
        )
    )


@pytest_asyncio.fixture(autouse=True)
async def _tables(pg_client: PostgresClient):
    await pg_client.execute("DROP TABLE IF EXISTS rt_mailbox;")
    await pg_client.execute("DROP TABLE IF EXISTS rt_cursors;")
    await pg_client.execute(_MAILBOX_DDL)
    await pg_client.execute(_CURSORS_DDL)
    yield


# ----------------------- #


@pytest.mark.asyncio
async def test_store_read_since_and_tenant_isolation(
    mailbox_ctx: ExecutionContext,
) -> None:
    ctx = mailbox_ctx

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx, retention=UNSWEPT)
        await mb.store(
            principal="u1", event_id=_eid(2), hlc=_hlc(2), signal=_signal("b")
        )
        await mb.store(
            principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a")
        )
        await mb.store(
            principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("a")
        )  # idempotent

        everything = await mb.read_since(principal="u1", since=None)
        after_e1 = await mb.read_since(principal="u1", since=_hlc(1))

        assert [e.event_id for e in everything] == [_eid(1), _eid(2)]  # ordered by hlc
        assert everything[0].payload == {"text": "a"}
        assert [e.event_id for e in after_e1] == [_eid(2)]  # strictly after

        assert (
            await mb.read_since(principal="u2", since=None) == []
        )  # principal isolation
        assert await mb.position_of(principal="u1", event_id=_eid(2)) == _hlc(2)
        assert await mb.position_of(principal="u1", event_id=_eid(99)) is None

    with _bind(ctx, _T2):  # a different tenant sees nothing — the adapter scopes it
        assert (
            await build_realtime_mailbox(ctx, retention=UNSWEPT).read_since(principal="u1", since=None)
            == []
        )


@pytest.mark.asyncio
async def test_replay_since_keyset_pages_over_postgres(
    mailbox_ctx: ExecutionContext,
) -> None:
    """``replay_since`` HLC keyset-pages correctly against real Postgres."""
    ctx = mailbox_ctx

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx, retention=UNSWEPT, replay_page_size=2)
        for n in range(1, 6):
            await mb.store(
                principal="u1", event_id=_eid(n), hlc=_hlc(n), signal=_signal(f"s{n}")
            )

        streamed = [
            e.event_id async for e in mb.replay_since(principal="u1", since=None)
        ]
        after = [
            e.event_id async for e in mb.replay_since(principal="u1", since=_hlc(3))
        ]

    # 5 rows streamed oldest-first across 3 keyset (`hlc > cursor`) pages of size 2.
    assert streamed == [_eid(n) for n in range(1, 6)]
    assert after == [_eid(4), _eid(5)]


@pytest.mark.asyncio
async def test_cursors_monotonic_min_and_ack_trim(
    mailbox_ctx: ExecutionContext,
) -> None:
    ctx = mailbox_ctx

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx, retention=UNSWEPT)
        cursors = build_realtime_cursors(ctx)

        for i in (1, 2, 3):
            await mb.store(
                principal="u1", event_id=_eid(i), hlc=_hlc(i), signal=_signal(str(i))
            )

        # monotonic cursor (update path under tenant_aware works on a real adapter)
        await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(2))
        await cursors.advance(
            principal="u1", client_key="d1", up_to=_hlc(1)
        )  # backwards
        assert await cursors.get(principal="u1", client_key="d1") == _hlc(2)

        # a slower second device drags the floor down
        await cursors.advance(principal="u1", client_key="d2", up_to=_hlc(1))
        assert await cursors.min_cursor(principal="u1") == _hlc(1)

        # trim what all devices have acked (floor = e1)
        await mb.trim(principal="u1", before=_hlc(1))
        remaining = await mb.read_since(principal="u1", since=None)
        assert [e.event_id for e in remaining] == [_eid(2), _eid(3)]


@pytest.mark.asyncio
async def test_equal_hlc_run_pages_without_skipping_on_postgres(
    mailbox_ctx: ExecutionContext,
) -> None:
    """The composite (hlc, id) keyset — an `$or` of range and tie-break — in real SQL."""
    ctx = mailbox_ctx

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx, retention=UNSWEPT, replay_page_size=2)
        for n in range(1, 6):  # one burst, one HLC — the wall-clock fallback shape
            await mb.store(
                principal="u1", event_id=_eid(n), hlc=_hlc(7), signal=_signal(f"s{n}")
            )

        streamed = [
            e.event_id async for e in mb.replay_since(principal="u1", since=None)
        ]

    # a page boundary inside the tie run resumes on the row id — nothing skipped
    assert streamed == [_eid(n) for n in range(1, 6)]


@pytest.mark.asyncio
async def test_overflow_window_keeps_the_newest_entries_on_postgres(
    mailbox_ctx: ExecutionContext,
) -> None:
    ctx = mailbox_ctx

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx, retention=UNSWEPT, cap=5, replay_page_size=2)
        for n in (1, 2, 3):
            await mb.store(
                principal="u1", event_id=_eid(n), hlc=_hlc(10), signal=_signal(f"s{n}")
            )
        for n in (4, 5, 6):
            await mb.store(
                principal="u1", event_id=_eid(n), hlc=_hlc(20), signal=_signal(f"s{n}")
            )

        streamed = [
            e.event_id async for e in mb.replay_since(principal="u1", since=None)
        ]

    # the cap boundary falls inside the hlc-10 group: the composite floor keeps the
    # newest five and loses exactly the group's oldest entry
    assert streamed == [_eid(n) for n in (2, 3, 4, 5, 6)]
    assert mb.stats().overflowed == 1


@pytest.mark.asyncio
async def test_retention_sweeps_scope_by_tenant_on_postgres(
    mailbox_ctx: ExecutionContext,
) -> None:
    """Age sweep + stale-cursor prune against real columns (bigint hlc, timestamptz)."""
    from datetime import timedelta

    from forze.base.primitives import utcnow

    ctx = mailbox_ctx

    with _bind(ctx, _T2):  # another tenant's ancient row must survive T1's sweep
        await build_realtime_mailbox(ctx, retention=UNSWEPT).store(
            principal="u1", event_id=_eid(9), hlc=_hlc(1), signal=_signal("other-tenant")
        )

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx, retention=UNSWEPT)
        cursors = build_realtime_cursors(ctx)

        await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("old"))
        await mb.store(principal="u2", event_id=_eid(2), hlc=_hlc(2), signal=_signal("old2"))
        await mb.store(principal="u1", event_id=_eid(3), hlc=_hlc(5000), signal=_signal("new"))
        await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(5000))

        deleted = await mb.sweep_older_than(cutoff=_hlc(3000))
        assert deleted == 2  # both principals' ancient entries, one pass
        remaining = await mb.read_since(principal="u1", since=None)
        assert [e.event_id for e in remaining] == [_eid(3)]
        assert await mb.read_since(principal="u2", since=None) == []

        # the prune filters on the row's own last_update_at (a real timestamptz)
        assert await cursors.prune_stale(idle_since=utcnow() - timedelta(days=1)) == 0
        assert await cursors.get(principal="u1", client_key="d1") is not None
        assert await cursors.prune_stale(idle_since=utcnow() + timedelta(days=1)) == 1
        assert await cursors.get(principal="u1", client_key="d1") is None

    with _bind(ctx, _T2):
        survivors = await build_realtime_mailbox(ctx, retention=UNSWEPT).read_since(principal="u1", since=None)
        assert [e.event_id for e in survivors] == [_eid(9)]  # the sweep never crossed tenants


# ----------------------- #
# sealing the stored signal bodies, against the real adapter


_SEALED_MAILBOX_DDL = """
CREATE TABLE rt_sealed_mailbox (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    tenant_id uuid NOT NULL,
    principal text NOT NULL,
    event_id text NOT NULL,
    hlc bigint NOT NULL,
    event text NOT NULL,
    payload jsonb NOT NULL
);
"""

_SEALED_SPEC = realtime_mailbox_spec(
    encryption=FieldEncryption(encrypted=frozenset({"payload"})),
)


@pytest_asyncio.fixture
async def sealed_mailbox_ctx(pg_client: PostgresClient) -> ExecutionContext:
    await pg_client.execute("DROP TABLE IF EXISTS rt_sealed_mailbox;")
    await pg_client.execute(_SEALED_MAILBOX_DDL)

    configurable = _configurable("rt_sealed_mailbox")

    return context_from_deps(
        Deps.merge(
            CryptoDepsModule(
                kms=MockKeyManagement(),
                directory=StaticKeyDirectory(KeyRef(key_id="mailbox-cmk")),
            )(),
            Deps.plain(
                {
                    PostgresClientDepKey: pg_client,
                    PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                }
            ),
            Deps.routed(
                {
                    DocumentQueryDepKey: {str(_SEALED_SPEC.name): configurable},
                    DocumentCommandDepKey: {str(_SEALED_SPEC.name): configurable},
                }
            ),
        )
    )


@pytest.mark.asyncio
async def test_a_sealed_mailbox_round_trips_a_json_boundary_payload(
    sealed_mailbox_ctx: ExecutionContext,
    pg_client: PostgresClient,
) -> None:
    """The encryption seam, driven through a real adapter rather than asserted on the spec.

    ``realtime_mailbox_spec(encryption=...)`` was only ever checked by reading the policy
    back off the spec, which proves the argument was stored, not that a sealed mailbox
    works. The payload is where that gap bites: it is typed ``JsonDict`` and crosses the
    codec, and this repo has already been caught once by a codec that keeps ``UUID`` /
    ``datetime`` / ``Decimal`` *live* in ``mode="python"`` — a lie the tests missed because
    every payload in them was ``str`` and ``int``. So this one carries all three, and the
    values are compared after the seal-store-fetch-unseal round trip, not before.
    """

    ctx = sealed_mailbox_ctx
    # LIVE objects where the boundary accepts them, not their string spellings: the
    # payload is typed ``JsonDict`` but nothing coerces it, so writing everything
    # pre-stringified is what hid this class of bug last time and would make the test
    # unable to fail for the reason it names. ``Decimal`` is deliberately the exception —
    # see the assertion at the end of this test, which pins it as the sharp edge it is.
    ref = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    at = datetime(2026, 8, 2, 10, 30, tzinfo=UTC)
    body = {"text": "sealed", "ref": ref, "at": at, "amount": "10.05", "count": 3}
    signal = RealtimeSignal.of(Audience.principal("u1"), "order.shipped", body)

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx, spec=_SEALED_SPEC, retention=UNSWEPT)
        await mb.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=signal)

        entries = await mb.read_since(principal="u1", since=None)

    assert [e.event_id for e in entries] == [_eid(1)]

    # Compared by VALUE across the boundary, not by identity: whatever spelling the codec
    # chose on the way out, the meaning has to come back intact.
    read = entries[0].payload
    assert read["text"] == "sealed"
    assert read["count"] == 3
    assert UUID(str(read["ref"])) == ref
    assert Decimal(str(read["amount"])) == Decimal("10.05")
    assert datetime.fromisoformat(str(read["at"])) == at

    # The index the mailbox filters and sorts on stays plaintext, or replay could not work
    # at all — and the body really is ciphertext at rest, which is the other half of the
    # claim and the half a decrypting read cannot make.
    rows = await pg_client.fetch_all("SELECT principal, hlc, payload::text FROM rt_sealed_mailbox")
    assert len(rows) == 1
    stored = dict(rows[0])
    assert stored["principal"] == "u1"
    # Stored packed, and stored as a NUMBER: sealed it would be text and the keyset
    # replay would have nothing to order by.
    assert stored["hlc"] == _hlc(1).pack()
    assert "sealed" not in stored["payload"], f"payload stored in the clear: {stored['payload']}"
    assert str(ref) not in stored["payload"]

    # A live ``Decimal`` in the payload is NOT supported, and fails badly: the field is
    # typed ``JsonDict``, nothing coerces or refuses it at ``RealtimeSignal.of``, and the
    # write dies deep in the codec with a bare ``TypeError`` rather than a CoreException
    # naming the field. UUID and datetime survive (orjson serializes both natively), which
    # is why only this one is pinned. Change this assertion when the boundary is fixed —
    # it is documenting a defect, not a guarantee.
    with pytest.raises(TypeError, match="Decimal"), _bind(ctx, _T1):
        await mb.store(
            principal="u1",
            event_id=_eid(2),
            hlc=_hlc(2),
            signal=RealtimeSignal.of(
                Audience.principal("u1"),
                "order.shipped",
                {"amount": Decimal("10.05")},
            ),
        )


@pytest.mark.asyncio
async def test_replay_and_ack_still_work_over_a_sealed_mailbox(
    sealed_mailbox_ctx: ExecutionContext,
) -> None:
    """Sealing the body must not disturb the replay index — the ordering and the since
    cursor are computed from ``hlc``/``event_id``, which the spec refuses to seal."""

    ctx = sealed_mailbox_ctx

    with _bind(ctx, _T1):
        mb = build_realtime_mailbox(ctx, spec=_SEALED_SPEC, retention=UNSWEPT)

        for n in (3, 1, 2):
            await mb.store(
                principal="u1",
                event_id=_eid(n),
                hlc=_hlc(n),
                signal=RealtimeSignal.of(
                    Audience.principal("u1"), "order.shipped", {"n": str(n)}
                ),
            )

        everything = await mb.read_since(principal="u1", since=None)
        tail = await mb.read_since(principal="u1", since=_hlc(1))

    assert [e.event_id for e in everything] == [_eid(1), _eid(2), _eid(3)]
    assert [e.event_id for e in tail] == [_eid(2), _eid(3)]
    assert [e.payload["n"] for e in everything] == ["1", "2", "3"]
